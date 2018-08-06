#!/bin/bash
# 建立Sqoop增量导入作业，以order_number作为检查列，初始的last-value是0
sqoop job --delete myjob_incremental_import
sqoop job --create myjob_incremental_import \
-- \
import \
--connect "jdbc:mysql://node5.bupt.edu.cn:3306/source?useSSL=false&user=root&password=1q2w3e4r" \
--table sales_order \
--columns "order_number, customer_number, product_code, order_date, entry_date, order_amount" \
--hive-import \
--hive-table rds.sales_order \
--incremental append \
--check-column order_number \
--last-value 0
# 首次抽取，将全部数据导入RDS库
sqoop import --connect jdbc:mysql://node5.bupt.edu.cn:3306/source?useSSL=false --username root --password 1q2w3e4r --table customer --hive-import --hive-table rds.customer --hive-overwrite
sqoop import --connect jdbc:mysql://node5.bupt.edu.cn:3306/source?useSSL=false --username root --password 1q2w3e4r --table product --hive-import --hive-table rds.product --hive-overwrite
beeline -u jdbc:hive2://node5.bupt.edu.cn:10000/dw -e "TRUNCATE TABLE rds.sales_order"
# 执行增量导入，因为last-value初始值为0，所以此次会导入全部数据
sqoop job --exec myjob_incremental_import
# 调用8-3-init_etl.sql文件执行初始装载
beeline -u jdbc:hive2://node5.bupt.edu.cn:10000/dw -f 8-3-init_etl.sql