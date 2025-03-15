# Проект 7-го спринта

## Этап 1
1. Скачиваем geo.csv
`wget -O geo.csv https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv`
Загружаем в HDFS
`hdfs dfs -copyFromLocal geo.csv /user/careenin/geo.csv/`

2. Читаем geo.csv, events.csv в Spark


`geo_df = spark.read.csv("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/careenin/geo.csv", header=True, inferSchema=True)
geo_df.show(5)

events_df = spark.read.parquet(
    "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/geo/events"
)
events_df.show(5)`

Проверяю структуру данных
`root
 |-- event: struct (nullable = true)
 |    |-- admins: array (nullable = true)
 |    |    |-- element: long (containsNull = true)
 |    |-- channel_id: long (nullable = true)
 |    |-- datetime: string (nullable = true)
 |    |-- media: struct (nullable = true)
 |    |    |-- media_type: string (nullable = true)
 |    |    |-- src: string (nullable = true)
 |    |-- message: string (nullable = true)
 |    |-- message_channel_to: long (nullable = true)
 |    |-- message_from: long (nullable = true)
 |    |-- message_group: long (nullable = true)
 |    |-- message_id: long (nullable = true)
 |    |-- message_to: long (nullable = true)
 |    |-- message_ts: string (nullable = true)
 |    |-- reaction_from: string (nullable = true)
 |    |-- reaction_type: string (nullable = true)
 |    |-- subscription_channel: long (nullable = true)
 |    |-- subscription_user: string (nullable = true)
 |    |-- tags: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- user: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- lat: double (nullable = true)
 |-- lon: double (nullable = true)
 |-- date: date (nullable = true)`

Меняем тип данных для использования математических функций, string не подойдёт, а также `,` нужно заменить на `.` для latitude/longitude.

Переименуем в geo_df колонки, чтобы избавиться от ошибки ambigious reference: lat -> city_lat, lng -> city_lng

Поскольку нас интересуют отправители, требуется фильтр message_from.isNotNull. Также выкину из исследования строки с Null в latitude/longitude. 

Сделала проверку на NULL по важным полям: message_from, lon, lan, date, datetime. Что странно, при не пустой date бывают NULL в datetime.

3. Планируем структуру хранилища
- Staging (исходные данные events; geo.csv)
- 

## Этап 2. Создаём витрину
Вычисляем расстояние между точкой события (сообщения) и городом по формуле на сфере (радиус Земли 6371).
Находим ближайший город для каждого сообщения через сортировку по дистанции.
Определяем актуальный город (последнее сообщение пользователя) и домашний (непрерывно ≥ 27 дней).
Собираем историю перемещений (список городов и их количество).

### Этап 3. Создаём витрину зон
Аналогичным образом привязываем каждое событие к ближайшему городу.
Считаем агрегаты по (city + неделя) и (city + месяц): количество сообщений, реакций, подписок, новых пользователей.
Формируем сводные поля week_message, week_reaction, week_subscription, week_user и аналогичные по месяцам.


### Этап 4. Витрина для рекомендации друзей
Находим пользователей, которые подписаны на один и тот же канал.
Исключаем тех, кто уже переписывался.
Вычисляем расстояние между оставшимися парами пользователей и фильтруем пары с дистанцией ≤ 1 км.
Формируем уникальные пары с user_left < user_right, добавляем поля zone_id, время расчёта и локальное время.

PS. Спринт очень объёмный, местами нелогичный и сложный. Проект объёмный и сложный... Было убито много нервных клеток из-за багов и подгорающих дедлайнов
