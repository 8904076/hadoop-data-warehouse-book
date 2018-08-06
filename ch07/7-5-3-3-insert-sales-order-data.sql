-- 往订单表中插入两条新数据
use source;
set @customer_number := floor(1 + rand() * 6);
set @product_code := floor(1 + rand() * 2);
set @order_date := from_unixtime(unix_timestamp('2016-07-03') + rand() * (unix_timestamp('2016-07-04') - unix_timestamp('2016-07-03')));
set @amount := floor(1000 + rand() * 9000);

insert into sales_order
values (101,@customer_number,@product_code,@order_date,@order_date,@amount);

set @customer_number := floor(1 + rand() * 6);
set @product_code := floor(1 + rand() * 2);
set @order_date := from_unixtime(unix_timestamp('2016-07-04') + rand() * (unix_timestamp('2016-07-05') - unix_timestamp('2016-07-04')));
set @amount := floor(1000 + rand() * 9000);

insert into sales_order
values (102,@customer_number,@product_code,@order_date,@order_date,@amount);

commit;