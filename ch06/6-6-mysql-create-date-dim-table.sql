use source;
-- 建立日期维度表
create table date_dim (
    date_sk int not null auto_increment primary key comment '代理键',
    day date comment '日期，yyyy-mm-dd',
    month tinyint comment '月份',
    month_name varchar(9) comment '月份名称',
    quarter tinyint comment '季度',
    year smallint comment '年'
);