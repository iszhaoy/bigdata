-- fliesystem
create table orders
(
    id       string,
    currency string,
    ts       string,
    proctime as proctime(),
    rowtime as COALESCE (to_timestamp(ts,'yyyy-MM-dd HH:mm:ss'),0),
    watermark for rowtime as rowtime - interval '2' second
) with (
      'connector' = 'filesystem',
      'path' = '/opt/module/data/orders.csv',
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true'
      );




--

CREATE
CATALOG myhive WITH (
    'type' = 'hive',
    'default-database' = 'flink_meta',
    'hive-conf-dir' = '/opt/module/flink-1.12-SNAPSHOT/hive'
);

-- es-sink upsert
create table agg_window_table_es
(
    id           string,
    cnt          bigint,
    cnt_distinct bigint,
    average      double,
    window_end   timestamp(3),
    primary key (id, window_end) not enforced -- 没有主键为追加 ，es会自动填充一个随机id
) with (
      'connector' = 'elasticsearch-7',
      'hosts' = 'http://192.168.149.1:9200',
      'index' = 'index_agg_window_table'
      );

-- es-sink upsert
create table agg_table_es
(
    id           string,
    cnt          bigint,
    cnt_distinct bigint,
    average      double,
    primary key (id) not enforced
) with (
      'connector' = 'elasticsearch-7',
      'hosts' = 'http://192.168.149.1:9200',
      'index' = 'index_agg_table'
      );

-- test_es_dml1
insert into agg_window_table_es
select id,
       count(*),
       count(distinct id) as count_distinct_id,
       avg(rate)          as avg_rate,
       tumble_end(rowtime, interval '1' minute)
from (select o.id, o.currency, o.rowtime, r.rate
      from orders as o
               left join rates for system_time as of o.proctime as r
                         on o.currency = r.currency)
group by id, tumble(rowtime, interval '1' minute);
-- test_es_dml2
insert into agg_table_es
select id,
       cast(count(id) as bigint)          as cnt,
       cast(count(distinct id) as bigint) as cnt_distinct,
       cast(avg(rate) as double)          as averge
from (select o.id, o.currency, o.rowtime, r.rate
      from orders as o
               left join rates for system_time as of o.proctime as r
                         on o.currency = r.currency)
group by id;

-- hbase
create table agg_window_table_hbase
(
    rowkey          string,
    f1              row<cnt bigint,
    cnt_distinct_id bigint,
    ave_rate        double>,
    primary key (rowkey) not enforced
) with (
      'connector' = 'hbase-1.4',
      'table-name' = 'agg_window_table_hbase',
      'zookeeper.quorum' = 'bigdata01:2181'
      )
-- test_hbase
    insert into agg_window_table_hbase
select id || '_' || date_format(window_end, 'yyyy-MM-dd'), Row(cnt, count_distinct_id, avg_rate)
from (
         select id,
                count(*)                                 as cnt,
                count(distinct id)                       as count_distinct_id,
                avg(rate)                                as avg_rate,
                tumble_end(rowtime, interval '1' minute) as window_end
         from (select o.id, o.currency, o.rowtime, r.rate
               from orders as o
                        left join rates for system_time as of o.proctime as r
                                  on o.currency = r.currency)
         group by id, tumble(rowtime, interval '1' minute)
     ) t

