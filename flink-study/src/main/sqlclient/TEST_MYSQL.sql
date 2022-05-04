-- mysql source
create table rates
(
    currency string,
    rate     double
)
    with (
        'connector' = 'jdbc',
        'url' = 'jdbc:mysql://192.168.149.2:3306/local?useSSL=false',
        'table-name' = 'rates',
        'username' = 'root',
        'password' = 'root',
        'lookup.cache.max-rows' = '100',
        'lookup.cache.ttl' = '10'
        );

---------------------------------

create table agg_window_table_mysql_upsert
(
    id           string,
    cnt          bigint,
    cnt_distinct bigint,
    average      double,
    window_end   timestamp,
    primary key (id,window_end)
)

---------------------------------
-- mysql sink upsert
drop table  if exists agg_window_table_mysql_upsert;
create table agg_window_table_mysql_upsert
(
    id           string,
    cnt          bigint,
    cnt_distinct bigint,
    average      double,
    window_end   timestamp(3),
    primary key (id, window_end) not enforced
) with (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://192.168.149.1:3306/local?useSSL=false',
      'table-name' = 'agg_window_table_mysql_upsert',
      'username' = 'root',
      'password' = 'root',
      'lookup.cache.max-rows' = '100',
      'lookup.cache.ttl' = '10'
      );


-- test

insert into agg_window_table_mysql_upsert
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
