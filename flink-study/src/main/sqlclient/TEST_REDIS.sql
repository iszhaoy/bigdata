
drop table  if exists agg_window_table_redis_upsert;
create table agg_window_table_redis_upsert
(
    hash_key     string,
    id     string,
    data_info          string,
    primary key (hash_key) not enforced
) with (
      'connector' = 'redis',
      'mode' = 'single',
      'single.host' = '192.168.149.80',
      'command' = 'HSET'
);

insert into agg_window_table_redis_upsert
select id || date_format( tumble_end(rowtime, interval '1' minute),'yyyyMMddHHmmss') as hash_key,
           'cnt:' || cast(count(*) as string)
           ||'count_distinct_id:' || cast(count(distinct id) as string)
           ||'avg_rate:' || cast(avg(rate) as string)  as data_info
from (select o.id, o.currency, o.rowtime, r.rate
      from orders as o
               left join rates for system_time as of o.proctime as r
      on o.currency = r.currency)
group by id, tumble(rowtime, interval '1' minute);



insert into agg_window_table_redis_upsert
select date_format( tumble_end(rowtime, interval '1' minute),'yyyyMMddHHmmss') as hash_key,
       id,
       'cnt:' || cast(count(*) as string)
           ||'count_distinct_id:' || cast(count(distinct id) as string)
           ||'avg_rate:' || cast(avg(rate) as string)  as data_info
from (select o.id, o.currency, o.rowtime, r.rate
      from orders as o
               left join rates for system_time as of o.proctime as r
                         on o.currency = r.currency)
group by id, tumble(rowtime, interval '1' minute);





