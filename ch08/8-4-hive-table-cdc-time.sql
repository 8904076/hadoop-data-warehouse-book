-- 建立时间戳表
use rds;
drop table if exists cdc_time ;
create table cdc_time
( last_load date, current_load date );

set hivevar:last_load = date_add(current_date(),-1);
insert overwrite table cdc_time select ${hivevar:last_load}, ${hivevar:last_load} ;