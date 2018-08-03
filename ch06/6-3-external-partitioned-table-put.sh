#!/bin/bash
# 设置环境变量
source /home/work/.bash_profile
# 取得前一天的日期，格式为yyyymmdd，作为分区的目录名
dt=$(date -d last-day +%Y%m%d)
# 建立HDFS目录
hadoop fs -mkdir -p /logs/$dt
# 将前一天的日志文件上传到HDFS的相应目录中
hadoop fs -put /data/statsvr/tmp/logs_$dt /logs/$dt
# 给Hive表增加一个新的分区，指向刚建的目录
hive --database logs -e "alter table logs add partition(dt=$dt) locacation 'hdfs://node5.bupt.edu.cn/logs/$dt'"