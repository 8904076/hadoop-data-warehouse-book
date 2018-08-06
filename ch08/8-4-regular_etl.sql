-- 定期装载 HiveQL
use dw;
-- 1、设置变量以支持事务
set hive.support.concurrency=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=1;

-- 2、设置数据处理时间窗口
-- 设置scd的生效时间和过期时间
set hivevar:cur_date = current_date();
set hivevar:pre_date = date_add(${hivevar:cur_date},-1);
set hivevar:max_date = cast('2200-01-01' as date);
-- 设置cdc的上限时间
insert overwrite table rds.cdc_time select last_load, ${hivevar:cur_date} from rds.cdc_time;

-- 3、装载客户维度表
-- 设置已删除记录和 customer_street_address 列上 scd2 的过期
update customer_dim
   set expiry_date = ${hivevar:pre_date}
 where customer_dim.customer_sk in
(select a.customer_sk
   from (select customer_sk,customer_number,customer_street_address
           from customer_dim where expiry_date = ${hivevar:max_date}) a left join
                rds.customer b on a.customer_number = b.customer_number
          where b.customer_number is null or a.customer_street_address <> b.customer_street_address);

-- 处理 customer_street_address 列上 scd2 的新增行
insert into customer_dim
select
    row_number() over (order by t1.customer_number) + t2.sk_max,
    t1.customer_number,
    t1.customer_name,
    t1.customer_street_address,
    t1.customer_zip_code,
    t1.customer_city,
    t1.customer_state,
    t1.version,
    t1.effective_date,
    t1.expiry_date
from
(
select
    t2.customer_number customer_number,
    t2.customer_name customer_name,
    t2.customer_street_address customer_street_address,
    t2.customer_zip_code,
    t2.customer_city,
    t2.customer_state,
    t1.version + 1 version,
    ${hivevar:pre_date} effective_date,
    ${hivevar:max_date} expiry_date
  from customer_dim t1
 inner join rds.customer t2
    on t1.customer_number = t2.customer_number
   and t1.expiry_date = ${hivevar:max_date}
  left join customer_dim t3
    on t1.customer_number = t3.customer_number
   and t3.expiry_date = ${hivevar:max_date}
 where t1.customer_street_address <> t2.customer_street_address and t3.customer_sk is null) t1
 cross join
 (select coalesce(max(customer_sk),0) sk_max from customer_dim) t2;

-- 处理 customer_name 列上的 scd1
-- 因为 scd1 本身就不保存历史数据，所以这里更新维度表里的
-- 所有 customer_name 改变的记录，而不是仅仅更新当前版本的记录
drop table if exists tmp;
create table tmp as
select
    a.customer_sk,
    a.customer_number,
    b.customer_name,
    a.customer_street_address,
    a.customer_zip_code,
    a.customer_city,
    a.customer_state,
    a.version,
    a.effective_date,
    a.expiry_date
  from customer_dim a, rds.customer b
 where a.customer_number = b.customer_number and (a.customer_name <> b.customer_name);
delete from customer_dim where customer_dim.customer_sk in (select customer_sk from tmp);
insert into customer_dim select * from tmp;

-- 处理新增的 customer 记录
insert into customer_dim
select
    row_number() over (order by t1.customer_number) + t2.sk_max,
    t1.customer_number,
    t1.customer_name,
    t1.customer_street_address,
    t1.customer_zip_code,
    t1.customer_city,
    t1.customer_state,
    1,
    ${hivevar:pre_date},
    ${hivevar:max_date}
from
(
select t1.* from rds.customer t1 left join customer_dim t2 on t1.customer_number =
t2.customer_number
 where t2.customer_sk is null) t1
 cross join
(select coalesce(max(customer_sk),0) sk_max from customer_dim) t2;

-- 4、装载产品维度表
-- 设置已删除记录和 product_name、product_category 列上 scd2 的过期
update product_dim
   set expiry_date = ${hivevar:pre_date}
 where product_dim.product_sk in
(select a.product_sk
   from (select product_sk,product_code,product_name,product_category
           from product_dim where expiry_date = ${hivevar:max_date}) a left join
                rds.product b on a.product_code = b.product_code
          where b.product_code is null or (a.product_name <> b.product_name or a.product_category <> b.product_category));

-- 处理 product_name、product_category 列上 scd2 的新增行
insert into product_dim
select
    row_number() over (order by t1.product_code) + t2.sk_max,
    t1.product_code,
    t1.product_name,
    t1.product_category,
    t1.version,
    t1.effective_date,
    t1.expiry_date
  from
(
select
    t2.product_code product_code,
    t2.product_name product_name,
    t2.product_category product_category,
    t1.version + 1 version,
    ${hivevar:pre_date} effective_date,
    ${hivevar:max_date} expiry_date
  from product_dim t1
 inner join rds.product t2
    on t1.product_code = t2.product_code
   and t1.expiry_date = ${hivevar:pre_date}
  left join product_dim t3
    on t1.product_code = t3.product_code
   and t3.expiry_date = ${hivevar:max_date}
 where (t1.product_name <> t2.product_name or t1.product_category <> t2.product_category) and
t3.product_sk is null) t1
cross join
(select coalesce(max(product_sk),0) sk_max from product_dim) t2;

-- 处理新增的 product 记录
insert into product_dim
select
    row_number() over (order by t1.product_code) + t2.sk_max,
    t1.product_code,
    t1.product_name,
    t1.product_category,
    1,
    ${hivevar:pre_date},
    ${hivevar:max_date}
  from
(
select t1.* from rds.product t1 left join product_dim t2 on t1.product_code = t2.product_code
 where t2.product_sk is null) t1
 cross join
(select coalesce(max(product_sk),0) sk_max from product_dim) t2;

-- 5、装载订单维度表
insert into order_dim
select
    row_number() over (order by t1.order_number) + t2.sk_max,
    t1.order_number,
    t1.version,
    t1.effective_date,
    t1.expiry_date
  from
(
select
    order_number order_number,
    1 version,
    order_date effective_date,
    '2200-01-01' expiry_date
  from rds.sales_order, rds.cdc_time
 where entry_date >= last_load and entry_date < current_load ) t1
 cross join
(select coalesce(max(order_sk),0) sk_max from order_dim) t2;

-- 6、装载销售订单事实表
insert into sales_order_fact
select
    order_sk,
    customer_sk,
    product_sk,
    date_sk,
    order_amount
  from
    rds.sales_order a,
    order_dim b,
    customer_dim c,
    product_dim d,
    date_dim e,
    rds.cdc_time f
 where
    a.order_number = b.order_number
and a.customer_number = c.customer_number
and a.order_date >= c.effective_date
and a.order_date < c.expiry_date
and a.product_code = d.product_code
and a.order_Date >= d.effective_date
and a.order_date < d.expiry_date
and to_date(a.order_date) = e.day
and a.entry_date >= f.last_load and a.entry_date < f.current_load ;

-- 7、更新数据处理时间窗口
-- 更新时间戳表的 last_load 字段
insert overwrite table rds.cdc_time select current_load, current_load from rds.cdc_time;
