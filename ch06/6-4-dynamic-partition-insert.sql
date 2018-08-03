-- 建立非分区表并装载数据
drop table if exists t1;
create table t1 (name string, cty string, st string)
row format delimited fields terminated by ',';
load data inpath '/user/test/b.txt' into table t1;
select * from t1;

-- 建立外部分区表并动态装载数据
drop table if exists t2;
create external table t2 (name string)
partitioned by (country string, state string);
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
insert into table t2 partition (country, state) 
select name, cty, st from t1;
insert into table t2 partition (country, state)
select name, cty, st from t1;
select * from t2;

-- 编辑数据文件后执行：
load data inpath '/user/test/b.txt' overwrite into table t1;
insert overwrite table t2 partition (country, state)
select name, cty, st from t1;
select * from t2;