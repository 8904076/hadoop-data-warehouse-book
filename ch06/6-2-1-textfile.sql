-- 根据实际目录添加hive-hcatalog-core.jar包：
add jar /opt/cloudera/parcels/CDH-5.15.0-1.cdh5.15.0.p0.21/lib/oozie/libtools/hive-hcatalog-core.jar
-- 建立测试表：
use test;
create table my_table(
    foo     string,
    bar     string,
    quux    struct<quuxid:int, quuxname:string>
)
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile;
-- 装载数据：
-- sudo -u hdfs hadoop fs -mkdir /user/test
-- sudo -u hdfs hadoop fs -put hivedata/simple.json /user/test/
load data inpath '/user/test/simple.json' into table my_table;
-- 查询：
select foo, bar, quux.quuxid, quux.quuxname from my_table;