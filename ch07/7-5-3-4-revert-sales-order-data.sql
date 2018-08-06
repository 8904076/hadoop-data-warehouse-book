-- 还原MySQL的 sales_order 表
use source;
delete from sales_order where order_number in(101,102);
alter table sales_order auto_increment=101;