use test;
-- 建立测试表：
create table t1(id int, name string)
clustered by (id) into 8 buckets
stored as orc tblproperties ('transactional'='true');

-- 测试insert... values 语句：
insert into t1 values (1,'aaa');
insert into t1 values (2,'bbb');

-- 查询结果：
select * from t1;

-- 测试update语句：
update t1 set name='ccc' where id=1;
select * from t1;

-- 测试delete语句：
delete from t1 where id=2;
select * from t1;

-- 测试从已有非ORC表装载数据：
-- sudo -u hdfs hadoop fs -put hivedata/a.txt /user/test/
use test;
drop table if exists t1;
create table t1 (id int, name string, cty string, st string)
row format delimited fields terminated by ',';
load data inpath '/user/test/a.txt' into table t1;

-- 建立外部分区事实表并装载数据：
create external table t2 (id int, name string)
partitioned by (country string, state string)
clustered by (id) into 8 buckets
stored as orc tblproperties ('transactional'='true');
insert into t2 partition (country, state) select * from t1;

-- 查询结果：
select * from t2;

-- 修改数据：
insert into table t2 partition (country, state) values (5,'e','dd','dd');
update t2 set name='f' where id=1;
delete from t2 where name='b';
select * from t2;