from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.appName("GeoEvents").getOrCreate()


def load_data(geo_path, events_path):
    geo_df = spark.read.csv(geo_path, header=True, sep=";", inferSchema=True)
    geo_df = (
        geo_df
        .withColumn("lat", F.regexp_replace("lat", ",", ".").cast("double"))
        .withColumn("lng", F.regexp_replace("lng", ",", ".").cast("double"))
        .withColumnRenamed("lat", "city_lat")
        .withColumnRenamed("lng", "city_lng")
    )

    events_df = spark.read.parquet(events_path)
    return geo_df, events_df

def calc_distance_expr(lat_col1, lon_col1, lat_col2, lon_col2, radius=6371):

    return (2 * F.lit(radius) *
            F.asin(
                F.sqrt(
                    F.pow(F.sin((F.radians(lat_col1) - F.radians(lat_col2)) / 2), 2) +
                    F.cos(F.radians(lat_col1)) * F.cos(F.radians(lat_col2)) *
                    F.pow(F.sin((F.radians(lon_col1) - F.radians(lon_col2)) / 2), 2)
                )
            ))


def process_events(geo_df, events_df):
    events_messages = events_df.filter(
        (F.col("event.message_from").isNotNull()) &
        (F.col("lat").isNotNull()) &
        (F.col("lon").isNotNull()) &
        (F.col("event_type") == 'message')
    ).select(
        F.col("event.message_from").alias("user_id"),
        F.coalesce(F.col("event.datetime"), F.col("event.message_ts")).alias("datetime"),
        F.col("event_type"),
        F.col("lat"),
        F.col("lon"),
        F.col("date")
    )

    joined_df = (
        events_messages
        .crossJoin(F.broadcast(geo_df))
        .withColumn(
            "distance",
            calc_distance_expr(F.col("lat"), F.col("lon"), F.col("city_lat"), F.col("city_lng"))
        )
    )

    w_dist = Window.partitionBy("user_id", "datetime", "lat", "lon").orderBy("distance")
    closest_city_df = joined_df.withColumn("rn", F.row_number().over(w_dist)).filter(F.col("rn") == 1)

    events_with_city_df = closest_city_df.select(
        "user_id",
        "datetime",
        "event_type",
        "lat",
        "lon",
        "date",
        "city"
    ).withColumn(
        "datetime_ts",
        F.to_timestamp(F.substring("datetime", 1, 19), "yyyy-MM-dd HH:mm:ss")
    )

    w_act = Window.partitionBy("user_id").orderBy(F.col("datetime_ts").desc_nulls_last())
    events_with_city_df = events_with_city_df.withColumn("rn_act", F.row_number().over(w_act))
    last_event_city_df = (
        events_with_city_df
        .filter(F.col("rn_act") == 1)
        .select("user_id", F.col("city").alias("act_city"), "datetime_ts")
        .withColumn("local_time", F.from_utc_timestamp("datetime_ts", "Australia/Sydney"))
    )

    daily_df = events_with_city_df.groupBy("user_id", "city", "date").agg(F.count("*").alias("cnt"))
    w_daily = Window.partitionBy("user_id", "city").orderBy("date")
    daily_df = (
        daily_df
        .withColumn("rnk", F.row_number().over(w_daily))
        .withColumn("day_idx", F.year("date") * 400 + F.dayofyear("date"))
        .withColumn("offset", F.col("day_idx") - F.col("rnk"))
    )

    streaks_df = (
        daily_df.groupBy("user_id", "city", "offset")
        .agg(
            F.count("*").alias("streak_length"),
            F.max("date").alias("last_date_in_streak")
        )
        .filter(F.col("streak_length") >= 27)
    )

    w_streak = Window.partitionBy("user_id").orderBy(F.col("last_date_in_streak").desc())
    home_city_df = (
        streaks_df
        .withColumn("rownum", F.row_number().over(w_streak))
        .filter(F.col("rownum") == 1)
        .select("user_id", F.col("city").alias("home_city"))
    )

    w_travel = Window.partitionBy("user_id").orderBy("datetime_ts").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    events_with_city_df = events_with_city_df.withColumn("city_list", F.collect_list("city").over(w_travel))

    w_desc = Window.partitionBy("user_id").orderBy(F.col("datetime_ts").desc_nulls_last())
    events_with_city_df = events_with_city_df.withColumn("rn2", F.row_number().over(w_desc))
    travel_df = (
        events_with_city_df
        .filter(F.col("rn2") == 1)
        .select("user_id", F.col("city_list").alias("travel_array"))
    )
    travel_df = travel_df.withColumn(
        "travel_count",
        F.size(F.array_distinct("travel_array"))
    )

    data_mart_df = (
        last_event_city_df
        .join(home_city_df, on="user_id", how="left")
        .join(travel_df, on="user_id", how="left")
        .select(
            "user_id",
            "act_city",
            "home_city",
            "travel_array",
            "travel_count",
            "local_time"
        )
    )

    return data_mart_df


def process_city_aggregates(geo_df, events_df):
    events_all = events_df.filter(
        (F.col("event.message_from").isNotNull()) &
        (F.col("lat").isNotNull()) &
        (F.col("lon").isNotNull())
    ).select(
        F.col("event.message_from").alias("user_id"),
        F.col("event_type"),
        F.coalesce(F.col("event.datetime"), F.col("event.message_ts")).alias("datetime"),
        F.col("lat"),
        F.col("lon")
    )

    joined_df = (
        events_all
        .crossJoin(F.broadcast(geo_df))
        .withColumn(
            "distance",
            calc_distance_expr(F.col("lat"), F.col("lon"), F.col("city_lat"), F.col("city_lng"))
        )
    )
    w_dist = Window.partitionBy("user_id", "datetime", "lat", "lon").orderBy("distance")
    closest_city_df = joined_df.withColumn("rn", F.row_number().over(w_dist)).filter(F.col("rn") == 1)

    events_with_city = (
        closest_city_df
        .select(
            F.col("user_id"),
            F.col("event_type"),
            F.col("city"),
            F.to_timestamp(F.substring("datetime", 1, 19), "yyyy-MM-dd HH:mm:ss").alias("datetime_ts")
        )
        .withColumn("year", F.year("datetime_ts"))
        .withColumn("week", F.weekofyear("datetime_ts"))
        .withColumn("month", F.month("datetime_ts"))
    )

    week_agg = (
        events_with_city
        .groupBy("city", "year", "week")
        .pivot("event_type", ["message", "reaction", "subscription", "registration"])
        .count()
    )
    week_agg = (
        week_agg
        .withColumnRenamed("message", "week_message")
        .withColumnRenamed("reaction", "week_reaction")
        .withColumnRenamed("subscription", "week_subscription")
        .withColumnRenamed("registration", "week_user")
        .fillna(0, subset=["week_message", "week_reaction", "week_subscription", "week_user"])
    )

    month_agg = (
        events_with_city
        .groupBy("city", "year", "month")
        .pivot("event_type", ["message", "reaction", "subscription", "registration"])
        .count()
    )
    month_agg = (
        month_agg
        .withColumnRenamed("message", "month_message")
        .withColumnRenamed("reaction", "month_reaction")
        .withColumnRenamed("subscription", "month_subscription")
        .withColumnRenamed("registration", "month_user")
        .fillna(0, subset=["month_message", "month_reaction", "month_subscription", "month_user"])
    )

    return week_agg, month_agg

def friend_recommendation_mart(subs_df, messages_df, user_city_df):

    same_channel_df = (
        subs_df.alias("t1")
        .join(subs_df.alias("t2"), on="channel_id", how="inner")
        .filter(F.col("t1.user_id") < F.col("t2.user_id")) 
        .select(
            F.col("t1.user_id").alias("user_left"),
            F.col("t2.user_id").alias("user_right"),
            F.col("t1.channel_id").alias("channel_id")
        )
        .distinct()
    )

    chatted_df = (
        messages_df
        .select(
            F.col("event.message_from").alias("from_user"),
            F.col("event.message_to").alias("to_user")
        )
        .filter(F.col("from_user").isNotNull() & F.col("to_user").isNotNull())
        .distinct()
    )
    chatted_swapped_df = (
        chatted_df.unionByName(
            chatted_df.select(
                F.col("to_user").alias("from_user"),
                F.col("from_user").alias("to_user")
            )
        )
    )
    not_chatted_df = same_channel_df.alias("pairs").join(
        chatted_swapped_df.alias("ch"),
        on=[F.col("pairs.user_left") == F.col("ch.from_user"),
            F.col("pairs.user_right") == F.col("ch.to_user")],
        how="leftanti"
    )


    user_city_left = user_city_df.select(
        F.col("user_id").alias("user_left"),
        F.col("city").alias("city_left"),
        F.col("lat").alias("lat_left"),
        F.col("lon").alias("lon_left"),
        F.col("time_zone").alias("tz_left")
    )
    user_city_right = user_city_df.select(
        F.col("user_id").alias("user_right"),
        F.col("city").alias("city_right"),
        F.col("lat").alias("lat_right"),
        F.col("lon").alias("lon_right"),
        F.col("time_zone").alias("tz_right")
    )

    pair_coords_df = (
        not_chatted_df
        .join(user_city_left, on="user_left", how="inner")
        .join(user_city_right, on="user_right", how="inner")
        .withColumn(
            "distance",
            calc_distance_expr(
                F.col("lat_left"), F.col("lon_left"),
                F.col("lat_right"), F.col("lon_right")
            )
        )
        .filter(F.col("distance") <= 1.0)
    )

    pair_coords_df = pair_coords_df.withColumn("zone_id", F.col("city_left"))

    pair_coords_df = (
        pair_coords_df
        .withColumn("processed_dttm", F.current_timestamp())
        .withColumn(
            "local_time",
            F.from_utc_timestamp(F.current_timestamp(), F.col("tz_left"))
        )
    )

    friend_reco_df = pair_coords_df.select(
        "user_left",
        "user_right",
        "processed_dttm",
        "zone_id",
        "local_time"
    )

    return friend_reco_df

def main():
    geo_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/careenin/geo.csv"
    events_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/geo/events"
    geo_df, events_df = load_data(geo_path, events_path)


    mode = sys.argv[1] if len(sys.argv) > 1 else None

    if mode == "process_events":
        final_users_df = process_events(geo_df, events_df)
        final_users_df.show(10, False)
        final_users_df.write.mode("overwrite").parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/careenin/datamarts/users_mart")

    elif mode == "process_city_aggregates":
        week_df, month_df = process_city_aggregates(geo_df, events_df)
        week_df.show(10, False)
        month_df.show(10, False)
        week_df.write.mode("overwrite").parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/careenin/datamarts/week_agg")
        month_df.write.mode("overwrite").parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/careenin/datamarts/month_agg")

    elif mode == "friend_recommendation_mart":
        final_friends_df = friend_recommendation_mart(events_df, events_df, geo_df)
        final_friends_df.show(10, False)
    else:
        print("nothing")


if __name__ == "__main__":
    main()
