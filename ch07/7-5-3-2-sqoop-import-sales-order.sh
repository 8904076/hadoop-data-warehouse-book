# 增量导入sales_order表
sqoop job --create myjob_1 \
-- \
import \
--connect "jdbc:mysql://node5.bupt.edu.cn:3306/source?useSSL=false&user=root&password=1q2w3e4r" \
--table sales_order \
--columns "order_number, customer_number, product_code, order_date, entry_date, order_amount" \
--where "entry_date < current_date()" \
--hive-import \
--hive-table rds.sales_order \
--incremental append \
--check-column entry_date \
--last-value '1900-01-01'

# 查看此时作业中保存的last-value
sqoop job --show myjob_1 | grep last.value

# 首次执行作业
sqoop job --exec myjob_1

# 查看首次导入后的last-value
sqoop job --show myjob_1 | grep last.value

# 源库增加两条数据（7-5-3-insert-sales-order-data.sql）
# -- 往订单表中插入两条新数据
# use source;
# set @customer_number := floor(1 + rand() * 6);
# set @product_code := floor(1 + rand() * 2);
# set @order_date := from_unixtime(unix_timestamp('2016-07-03') + rand() * (unix_timestamp('2016-07-04') - unix_timestamp('2016-07-03')));
# set @amount := floor(1000 + rand() * 9000);

# insert into sales_order
# values (101,@customer_number,@product_code,@order_date,@order_date,@amount);

# set @customer_number := floor(1 + rand() * 6);
# set @product_code := floor(1 + rand() * 2);
# set @order_date := from_unixtime(unix_timestamp('2016-07-04') + rand() * (unix_timestamp('2016-07-05') - unix_timestamp('2016-07-04')));
# set @amount := floor(1000 + rand() * 9000);

# insert into sales_order
# values (102,@customer_number,@product_code,@order_date,@order_date,@amount);

# commit;

# 再次执行sqoo作业
sqoop job --exec myjob_1

# 查看此时的last-value
sqoop job --show myjob_1 | grep last.value

# 在hive的rds库里查询
# select * from rds.sales_order order by order_number desc;

# 还原数据（7-5-3-revert-sales-order-data.sql）
# -- 还原MySQL的 sales_order 表
# use source;
# delete from sales_order where order_number in(101,102);
# alter table sales_order auto_increment=101;