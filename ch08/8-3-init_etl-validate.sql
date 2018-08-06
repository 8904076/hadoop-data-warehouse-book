-- 验证初始装载的正确性
use dw;
select order_number, customer_name, product_name, day,
       order_amount amount
    from sales_order_fact a, customer_dim b, product_dim c,
         order_dim d, date_dim e
    where a.customer_sk = b.customer_sk
      and a.product_sk = c.product_sk
      and a.order_sk = d.order_sk
      and a.order_date_sk = e.date_sk
    order by order_number;