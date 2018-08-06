-- 建立日期维度数据生成的存储过程
delimiter //
drop procedure if exists pre_populate_date //
create procedure pre_populate_date (in start_dt date, in end_dt date)
begin
    while start_dt <= end_dt do
        insert into date_dim(date_sk, day, month, month_name, quarter, year)
        values(null, start_dt, month(start_dt), monthname(start_dt), quarter(start_dt), year(start_dt));
        set start_dt = adddate(start_dt, 1);
end while;
commit;
end
//
delimiter ;

-- 生成日期维度数据
set foreign_key_checks=0;
truncate table date_dim;
call pre_populate_date('2000-01-01', '2020-12-31');
set foreign_key_checks=1;