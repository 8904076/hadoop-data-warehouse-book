-- 一、去除完全重复的记录
select distinct * from t;

-- 二、去除部分重复的记录
-- Oracle、MySQL，使用相关子查询
select * from t t1
    where t1.id =
    (select min(t2.id)
        from t t2
     where t1.name = t2.name and t1.address = t2.address);