-- 建立TEXTFILE格式的表
-- 建立TEXTFILE格式的表
use test;
create table t_textfile(c1 string, c2 int, c3 string, c4 string)
row format delimited fields terminated by ',' stored as textfile
;
-- 向表中导入数据：
-- sudo -u hdfs hadoop fs -mkdir /user/test
-- sudo -u hdfs hadoop fs -put hivedata/data.csv /user/test
load data inpath '/user/test/data.csv' into table test.t_textfile;
-- 查询表：
select * from t_textfile;