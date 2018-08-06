-- Hive只支持在FROM子句中使用子查询，子查询必须有名字，并且列必须唯一
select t1.*
    from t t1,
        (select name, address, min(id) id from t group by name, address) t2
    where t1.id = t2.id;

-- 还可以使用Hive的row_number()分析函数
select t.id, t.name, t.address
    from (select id, name, address,
row_number() over (distribute by name, address sort by id) as rn from t) t
    where t.rn = 1;