-- 建立数据仓库数据库
drop database if exists dw cascade;
create database dw;

use dw;
-- 建立日期维度表
create table date_dim (
    date_sk int comment 'surrogate key',
    day date comment 'date,yyyy-mm-dd',
    month tinyint comment 'month',
    month_name varchar(9) comment 'month name',
    quarter tinyint comment 'quarter',
    year smallint comment 'year'
)
comment 'date dimension table'
row format delimited fields terminated by ','
stored as textfile;

-- 建立客户维度表
create table customer_dim (
    customer_sk int comment 'surrogate key',
    customer_number int comment 'number',
    customer_name varchar(50) comment 'name',
    customer_street_address varchar(50) comment 'address',
    customer_zip_code int comment 'zipcode',
    customer_city varchar(30) comment 'city',
    customer_state varchar(2) comment 'state',
    version int comment 'version',
    effective_date date comment 'effective date',
    expiry_date date comment 'expiry date'
)
clustered by (customer_sk) into 8 buckets
stored as orc tblproperties ('transactional'='true');

-- 建立产品维度表
create table product_dim (
    product_sk int comment 'surrogate key',
    product_code int comment 'code',
    product_name varchar(30) comment 'name',
    product_category varchar(30) comment 'category',
    version int comment 'version',
    effective_date date comment 'effective date',
    expiry_date date comment 'expiry date'
)
clustered by (product_sk) into 8 buckets
stored as orc tblproperties('transactional'='true');

-- 建立订单维度表
create table order_dim (
    order_sk int comment 'surrogate key',
    order_number int comment 'order number',
    version int comment 'version',
    effective_date date comment 'effective date',
    expiry_date date comment 'expiry date'
)
clustered by (order_sk) into 8 buckets
stored as orc tblproperties('transactional'='true');

-- 建立销售订单事实表
create table sales_order_fact (
    order_sk int comment 'order surrogate key',
    customer_sk int comment 'customer surrogate key',
    product_sk int comment 'product surrogate key',
    order_date_sk int comment 'date surrogate key',
    order_amount decimal(10,2) comment 'order amount'
)
clustered by (order_sk) into 8 buckets
stored as orc tblproperties('transactional'='true');
