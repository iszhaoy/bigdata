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
    partitioned by (month string)
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
