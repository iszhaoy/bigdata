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


-- filesystem sink
create table orders_hdfs
(
    id       string,
    currency string,
    dt       string,
    `hour`   string,
    `min`    string
) partitioned by (dt, `hour`,`min`) with (
  'connector'='filesystem',
  'path'='hdfs:///sql_client/orders_fs',
  'format'='csv',
  'sink.partition-commit.trigger'='process-time',
  'sink.partition-commit.delay'='10s',
  'sink.partition-commit.policy.kind'='success-file'
);

INSERT INTO orders_hdfs
SELECT id, currency, DATE_FORMAT(proctime, 'yyyy-MM-dd'), DATE_FORMAT(proctime, 'HH'), DATE_FORMAT(proctime, 'mm')
FROM orders_kafka;