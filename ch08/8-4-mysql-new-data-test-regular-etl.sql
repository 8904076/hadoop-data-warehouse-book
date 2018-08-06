-- mysql 的 source 源数据库中增加客户、产品和销售订单测试数据
use source;

/*** 客户的数据的改变如下：
客户 6 的街道号改为 7777 ritter rd。（原来是 7070 ritter rd）
客户 7 的姓名改为distinguished agencies。（原来是 distinguished partners）
新增第 8 个客户。
***/
update customer set customer_street_address = '7777 ritter rd.' where customer_number = 6 ;
update customer set customer_name = 'distinguished agencies' where customer_number = 7 ;
insert into customer (customer_name, customer_street_address, customer_zip_code,
customer_city, customer_state)
values ('subsidiaries', '10000 wetline blvd.', 17055, 'pittsburgh', 'pa') ;

/*** 产品的数据的改变如下：
产品 3 的名称改为 flat panel。（原来是 lcd panel）
新增第四个产品。
***/
update product set product_name = 'flat panel' where product_code = 3 ;
insert into product (product_name, product_category)
values ('keyboard', 'peripheral') ;

/*** 新增订单日期为 2016 年 7 月 4 日的 16 条订单。***/
drop procedure if exists generate_sales_order_data;
delimiter //
create procedure generate_sales_order_data()
begin
    drop table if exists temp_sales_order_data;
    create table temp_sales_order_data as select * from sales_order where 1=0; 

        alter table sales_order auto_increment=101;
        alter table temp_sales_order_data auto_increment=101;

        -- set @start_date := unix_timestamp('2016-07-04');
        -- set @end_date := unix_timestamp('2016-07-05');
        set @start_date := unix_timestamp('2018-08-05');
        set @end_date := unix_timestamp('2018-08-06');
        set @i := 101; 
        while @i<=116 do
            set @customer_number := floor(1 + rand() * 6);
            set @product_code := floor(1 + rand() * 2);
            set @order_date := from_unixtime(@start_date + rand() * (@end_date - @start_date));
            set @amount := floor(1000 + rand() * 9000);

            insert into temp_sales_order_data values (@i,@customer_number,@product_code,@order_date,@order_date,@amount);
            set @i:=@i+1;
        end while;

        insert into sales_order
        select null,customer_number,product_code,order_date,entry_date,order_amount from temp_sales_order_data order by order_date;
        commit;
end
//
delimiter ;

call generate_sales_order_data();
