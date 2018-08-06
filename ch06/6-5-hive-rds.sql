-- 建立rds数据库
drop database if exists rds cascade;
create database rds;

use rds;
-- 建立客户过渡表
create table customer (
    customer_number int comment 'number',
    customer_name varchar(50) comment 'name',
    customer_street_address varchar(50) comment 'address',
    customer_zip_code int comment 'zipcode',
    customer_city varchar(30) comment 'city',
    customer_state varchar(2) comment 'state'
);
-- 建立产品过渡表
create table product (
    product_code int comment 'code',
    product_name varchar(30) comment 'name',
    product_category varchar(30) comment 'category'
);
-- 建立销售订单过渡表
create table sales_order (
    order_number int comment 'order number',
    customer_number int comment 'customer number',
    product_code int comment 'product code',
    order_date timestamp comment 'order date',
    entry_date timestamp comment 'entry date',
    order_amount decimal(10,2) comment 'order amount'
);