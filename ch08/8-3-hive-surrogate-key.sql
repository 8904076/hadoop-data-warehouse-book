-- 将过渡表tbl_stg的数据装载到维度表tbl_dim，装载的同时生成维度表的代理键
use test;
-- 创建表tbl_stg
create table tbl_stg (id int, name string, address string)
row format delimited fields terminated by ',';

-- 装载测试数据到tbl_stg
-- 1,jack,aaa
-- 2,lily,bbb
-- sudo -u hdfs hdfs dfs -put stg.csv /user/test
load data inpath '/user/test/stg.csv' into table tbl_stg;

-- 创建表tbl_dim
create table tbl_dim (sk int, id int, name string, address string)
row format delimited fields terminated by ',';

-- 1、用row_number()函数生成代理键
insert into tbl_dim
select row_number() over (order by tbl_stg.id) + t2.sk_max, tbl_stg.*
from tbl_stg
cross join (select coalesce(max(sk),0) sk_max from tbl_dim) t2;

-- 2、用UDFRowSequence生成代理键
-- sudo -u hdfs hadoop fs -mkdir /user/jars
-- sudo -u hdfs hadoop fs -put /opt/cloudera/parcels/CDH-5.15.0-1.cdh5.15.0.p0.21/jars/hive-contrib-1.1.0-cdh5.15.0.jar /user/jars
add jar hdfs://node5.bupt.edu.cn:8020/user/jars/hive-contrib-1.1.0-cdh5.15.0.jar;
create temporary function row_sequence as
'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';

insert into tbl_dim
select row_sequence() + t2.sk_max, tbl_stg.*
from tbl_stg
cross join (select coalesce(max(sk), 0) sk_max from tbl_dim) t2;