#!/bin/bash
# 整体拉取 customer、product 表数据
sqoop import --connect jdbc:mysql://node5.bupt.edu.cn:3306/source?useSSL=false --username root --password 1q2w3e4r --table customer --hive-import --hive-table rds.customer --hive-overwrite
sqoop import --connect jdbc:mysql://node5.bupt.edu.cn:3306/source?useSSL=false --username root --password 1q2w3e4r --table product --hive-import --hive-table rds.product --hive-overwrite
# 执行增量导入
sqoop job --exec myjob_incremental_import
# 调用 8-4-regular_etl.sql 文件执行定期装载
beeline -u jdbc:hive2://node5.bupt.edu.cn:10000/dw -f 8-4-regular_etl.sql