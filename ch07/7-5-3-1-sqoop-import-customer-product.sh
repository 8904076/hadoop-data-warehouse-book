# 覆盖导入customer表
sqoop import \
--connect jdbc:mysql://node5.bupt.edu.cn:3306/source?useSSL=false \
--username root \
--password 1q2w3e4r \
--table customer \
--hive-import \
--hive-table rds.customer \
--hive-overwrite
# 覆盖导入product表
sqoop import \
--connect jdbc:mysql://node5.bupt.edu.cn:3306/source?useSSL=false \
--username root \
--password 1q2w3e4r \
--table product \
--hive-import \
--hive-table rds.product \
--hive-overwrite