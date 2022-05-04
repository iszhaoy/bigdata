-- 查看表结构
show create table test;
desc formatted test;

-- 创建复杂结构的数据
-- songsong,bingbing_lili,xiao song:18_xiaoxiao song:19,hui long
-- guan_beijing
-- yangyang,caicai_susu,xiao yang:18_xiaoxiao yang:19,chao
-- yang_beijing

create table test
(
    name     string,
    friends  array<string>,
    children map<string,int>,
    addredd  struct<street:string, city:string>
)
    row format delimited
        fields terminated by ','
        collection items terminated by '_'
        map keys terminated by ':'
        lines terminated by '\n';

-- 分区表
create external table test_partition
(
    deptno int,
    dname  string,
    loc    string
)
    partitioned by (`month` string)
    row format delimited
        fields terminated by '\t';

-- 增加分区
alter table test_partition
    add partition (month = '202002');
-- 删除分区
alter table test_partition
    drop partition (month = '202002');
-- 查看分区
show partitions test_partition;
-- 修复命令
msck repair table test_partition;
-- 添加列
alter table test_partition
    add columns (deptdesc string);
-- 更新列
alter table test_partition
    change column deptdesc desc int;
-- 替换列
alter table test_partition
    replace columns ( deptno int, dname string,loc string);

-- 向表中导入数据
load data local inpath '/xxx' into table test_partition partition (month = '201709',day = '10');

-- 插入 into overwrite
insert into table test_partition partition (month = "202002")
select *
from test_partition
where month = "201812";

-- 多表结果插入
-- from student
--     insert overwrite table student
--     partition(month='201707')
--     select id, name where month='201709'
--     insert overwrite table student
--     partition(month='201706')
--     select id, name where month='201709';

-- 查询结果导入 导出
insert overwrite local directory
    '/opt/module/datas/export/student'
select *
from student;

insert overwrite local directory
    '/opt/module/datas/export/student1'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
select *
from student;

export table student to
    '/user/hive/warehouse/export/student';

import table student2 partition (month='201709')
    from
        '/user/hive/warehouse/export/student';

-- Hive 支持通常的 SQL JOIN 语句，但是只支持等值连接，不支持非等值连接。
-- dept         emp
-- 001 A        e1   002
-- 002 B        e2   002
--              e3   001
select d.id, d.name, e.d_id, e.name
from dept d
         left join emp e
where d.id != e.d_id;

-- emp.d_id != dept.id
-- A   e1
-- A   e2
-- B   e3
create table dept
(
    id   string,
    name string
)
    row format delimited
        fields terminated by '\t';

create table emp
(
    name string,
    d_id string
)
    row format delimited
        fields terminated by '\t';

-- reducer 内部排序
select *
from emp sort by d_id desc;


-- 分区排序
create table distribute_table
(
    merid string,
    money string,
    name  string
) row format delimited
    fields terminated by '\t';

load data local inpath '/usr/soft/hive/data/distribute.txt' into table distribute_table;

set mapreduce.job.reduces=2;

select *
from distribute_table distribute by merid sort by money desc;

-- casewhen
-- 求出不同部门男女各多少人。
-- 悟空	A	男
-- 大海	A	男
-- 宋宋	B	男
-- 凤姐	A	女
-- 婷姐	B	女
-- 婷婷	B	女

create table casewhen_table
(
    name    string,
    dept_id string,
    sex     string
) row format delimited
    fields terminated by '\t';

load data local inpath '/usr/soft/hive/data/casewhen.txt' into table casewhen_table;

select dept_id,
       sum(case when sex = '男' then 1 else 0 end) as male_count,
       sum(case when sex = '女' then 1 else 0 end) as female_count
from casewhen_table
group by dept_id;

-- lateral view
-- 《疑犯追踪》 悬疑,动作,科幻,剧情
-- 《 Lie to me》 悬疑,警匪,动作,心理,剧情
-- 《战狼 2》 战争,动作,灾难

create table lateralview_table
(
    movie    string,
    category string
)
    row format delimited
        fields terminated by '\t';
load data local inpath '/usr/soft/hive/data/lateralview.txt' into table lateralview_table;

select movie,
       category_name
from lateralview_table lateral view explode(split(category, ',')) table_tmp as category_name;


-- 窗口函数 over
-- jack,2017-01-01,10
-- tony,2017-01-02,15
-- jack,2017-02-03,23
-- tony,2017-01-04,29
-- jack,2017-01-05,46
-- jack,2017-04-06,42
-- tony,2017-01-07,50
-- jack,2017-01-08,55
-- mart,2017-04-08,62
-- mart,2017-04-09,68
-- neil,2017-05-10,12
-- mart,2017-04-11,75
-- neil,2017-06-12,80
-- mart,2017-04-13,94

create table order_table
(
    name string,
    data string,
    cost int
) row format delimited
    fields terminated by ',';
load data local inpath '/usr/soft/hive/data/over.txt' into table order_table;

-- （ 1） 查询在 2017 年 4 月份购买过的顾客及总人数

select name,
       count(*) over ()
from order_table
where substring(data, 1, 7) = '2017-04'
group by name;

-- （ 2） 查询每一个顾客的购买明细及他的月购买总额
select name,
       data,
       cost,
       sum(cost) over (partition by name,month(data)) as total_month
from order_table;


-- （ 3） 上述的场景,要将 cost 按照日期进行累加

select name,
       data,
       cost,
       sum(cost) over (),
       sum(cost) over (partition by name),
       sum(cost) over (partition by name order by data)
from order_table;


-- sum(cost) over () 将所有行的cost加起来
-- sum(cost) over(partition by name), 通过name分区，将分区内的cost加起来
-- sum(cost) over(partition by name  order by data), 通过name分区，将分区内的cost累加起来

-- （ 4） 查询顾客上次的购买时间

select name,
       data,
       cost,
       lag(data, 1, '1990-01-01') over (partition by name order by data) as pre_data1,
       lag(data, 2) over (partition by name order by data)               as pre_data2
from order_table;


-- （ 5） 查询前 20%时间的订单信息
select name, data, cost
from (
         select name, data, cost, ntile(5) over (order by data) as sorted from order_table
     ) t
where t.sorted = 1;


-- rank排序
-- 孙悟空 语文 87
-- 孙悟空 数学 95
-- 孙悟空 英语 68
-- 大海 语文 94
-- 大海 数学 56
-- 大海 英语 84
-- 宋宋 语文 64
-- 宋宋 数学 86
-- 宋宋 英语 84
-- 婷婷 语文 65
-- 婷婷 数学 85
-- 婷婷 英语 78
create table score
(
    name    string,
    subject string,
    score   int
)
    row format delimited fields terminated by "\t";
load data local inpath '/usr/soft/hive/data/rank.txt' into table
    score;

-- rank 排序相同时会重复，总数不会变
-- dense_rank 排序相同时会重复，总数会减少
-- row_number 会根据顺序计算
select name,
       subject,
       score,
       rank() over (partition by subject order by score desc)       rp,
       dense_rank() over (partition by subject order by score desc) drp,
       row_number() over (partition by subject order by score desc) rmp
from score;

-- 压缩格式和文件格式
CREATE TABLE `table_textFile`
(
    `merid` string,
    `money` string,
    `name`  string
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    stored as textfile;

load data local inpath '/usr/soft/hive/data/log.txt' into table table_textFile;


CREATE TABLE `table_orc`
(
    `merid` string,
    `money` string,
    `name`  string
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    stored as orc;

-- 不能使用load加载数据，因为load相当于 hadoop put
insert into table_orc
select *
from table_textFile;

CREATE TABLE `table_parquet`
(
    `merid` string,
    `money` string,
    `name`  string
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    stored as parquet;

insert into table_parquet
select *
from table_textFile;


-- Hive 实战之谷粒影音

-- yarn jar /usr/soft/hive/data/hadoop-demo-1.0-SNAPSHOT.jar \
-- com.iszhaoy.guli.GuLiJob \
-- /guli_demo/video \
-- /guli_demo/output/video


create table gulivideo_ori
(
    videoId   string,
    uploader  string,
    age       int,
    category  array<string>,
    length    int,
    views     int,
    rate      float,
    ratings   int,
    comments  int,
    relatedId array<string>
)
    row format delimited
        fields terminated by "\t"
        collection items terminated by "&"
    stored as textfile;

create table gulivideo_user_ori
(
    uploader string,
    videos   int,
    friends  int
)
    row format delimited
        fields terminated by "\t"
    stored as textfile;

load data inpath '/guli_demo/video/2008/0222' into table gulivideo_ori;

load data inpath '/guli_demo/user/*' into table gulivideo_user_ori;


create table gulivideo_orc
(
    videoId   string,
    uploader  string,
    age       int,
    category  array<string>,
    length    int,
    viewers   int,
    rate      float,
    ratings   int,
    comments  int,
    relatedId array<string>
)
    row format delimited
        fields terminated by "\t"
        collection items terminated by "&"
    stored as orc;

create table gulivideo_user_orc
(
    uploader string,
    videos   int,
    friends  int
)
    row format delimited
        fields terminated by "\t"
    stored as orc;

insert overwrite table gulivideo_orc
select *
from gulivideo_ori;

insert overwrite table gulivideo_user_orc
select *
from gulivideo_user_ori;

--统计视频观看数 Top10
select videoId,
       uploader,
       age,
       category,
       length,
       viewers,
       rate,
       ratings,
       comments
from gulivideo_orc
order by viewers desc
limit 10;
--统计视频类别热度 Top10

select cate, count(*) as count_num
from (
         select explode(category) as cate
         from gulivideo_orc
     ) t1
group by t1.cate
order by count_num desc
limit 10;

--统计视频观看数 Top20 所属类别以及类别包含的 Top20 的视频个数
select category_name as category, count(t2.videoId) as hot_with_views
from (
         select t1.videoId, category_name
         from (
                  select *
                  from gulivideo_orc
                  order by `viewers` desc
                  limit 20
              ) t1 lateral view explode(category) table_tmp as category_name
     ) t2
group by category_name
order by hot_with_views desc;

--统计视频观看数 Top50 所关联视频的所属类别 Rank
select cate, count(*) as rank
from (
         select explode(t2.category) as cate
         from (
                  select distinct t5.r_video_id
                  from (
                           select explode(relatedId) as r_video_id
                           from (select viewers, relatedid
                                 from gulivideo_orc
                                 order by viewers desc
                                 limit 50) t1
                       ) t5
              ) t3
                  inner join gulivideo_orc t2
                             on t3.r_video_id = t2.videoId
     ) t4
group by cate
order by rank desc;

select category_name as category,
       count(*)      as hot
from (
         select category_name
         from (
                  select distinct(t2.videoId),
                                 t3.category
                  from (
                           select explode(relatedId) as videoId
                           from (
                                    select *
                                    from gulivideo_orc
                                    order by viewers
                                        desc
                                    limit
                                        50) t1) t2
                           inner join
                       gulivideo_orc t3 on t2.videoId = t3.videoId) t4 lateral view
             explode(category) t_catetory as category_name) t5
group by category_name
order by hot
    desc;

--统计每个类别中的视频热度 Top10
create table gulivideo_category
(
    videoId    string,
    uploader   string,
    age        int,
    categoryId string,
    length     int,
    viewers    int,
    rate       float,
    ratings    int,
    comments   int,
    relatedId  array<string>
)
    row format delimited
        fields terminated by "\t"
        collection items terminated by "&"
    stored as orc;

insert into table gulivideo_category
select videoId,
       uploader,
       age,
       categoryId,
       length,
       viewers,
       rate,
       ratings,
       comments,
       relatedId
from gulivideo_orc lateral view explode(category) catetory as
         categoryId;

select videoId,
       viewers
from gulivideo_category
where categoryId = "Music"
order by viewers
    desc
limit
    10;

--统计每个类别中视频流量 Top10
select videoId,
       viewers,
       ratings
from gulivideo_category
where categoryId = "Music"
order by ratings
    desc
limit
    10;

--统计上传视频最多的用户 Top10 以及他们上传的观看次数在前 20 视频

select videoId,
       viewers,
       ratings
from gulivideo_category
where categoryId = "Music"
order by ratings
    desc
limit
    10;
--统计每个类别视频观看数 Top10
select t1.*
from (
         select videoId,
                categoryId,
                viewers,
                row_number() over (partition by categoryId order by viewers desc)
                    rank
         from gulivideo_category) t1
where rank <= 10;
