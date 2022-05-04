-- kafka source
create table orders_kafka
(
    id       string,
    currency string,
    ts       string,
    proctime as proctime(),
    rowtime as coalesce (to_timestamp(ts,'yyyy-MM-dd HH:mm:ss'),0),
    watermark for rowtime as rowtime - interval '0' second
) with (
      'connector' = 'kafka',
      'topic' = 'Orders',
      'scan.startup.mode' = 'earliest-offset', -- group-offsets
      'properties.bootstrap.servers' = 'kafka:9092',
      'properties.group.id' = 'flinkSQLGroup',
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true'
      );

-- kafka upsert sink
drop table if exists agg_window_table_kafka_upsert;
create table agg_window_table_kafka_upsert
(
    id           string,
    cnt          bigint,
    cnt_distinct bigint,
    average      double,
    window_end   timestamp(3),
    primary key (id, window_end) not enforced
) with (
      'connector' = 'upsert-kafka',
      'topic' = 'agg_window_table_kafka_upsert',
      'properties.bootstrap.servers' = 'kafka:9092',
      'properties.group.id' = 'flinkSQLGroup',
      'key.format' = 'avro',
      'value.format' = 'avro'
      );

drop table if exists agg_window_table_kafka_upsert_csv;
create table agg_window_table_kafka_upsert_csv
(
    id            string,
    cnt           bigint,
    cnt_distinct  bigint,
    average       double,
    cur_timestamp timestamp(3),
    primary key (id) not enforced
) with (
      'connector' = 'upsert-kafka',
      'topic' = 'agg_window_table_kafka_upsert_csv',
      'properties.bootstrap.servers' = 'kafka:9092',
      'properties.group.id' = 'flinkSQLGroup',
      'key.format' = 'csv',
      'value.format' = 'csv'
      );

-- test
insert into agg_window_table_kafka_upsert_csv
select '1',
       count(*),
       count(distinct id) as count_distinct_id,
       avg(rate)          as avg_rate,
       current_timestamp  as cur_timestamp
from (select o.id, o.currency, o.rowtime, r.rate
      from orders_kafka as o
               left join rates for system_time as of o.proctime as r
                         on o.currency = r.currency);