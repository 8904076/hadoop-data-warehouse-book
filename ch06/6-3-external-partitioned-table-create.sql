-- 创建日志外部表
create external table logs(
    platform        string,
    createtime      string,
    channel         string,
    product         string,
    userid          string,
    content         map<string,string>)
partitioned by (dt int)
row format delimited fields terminated by '\t'
location 'hdfs://node5.bupt.edu.cn/logs';