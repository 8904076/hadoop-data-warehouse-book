-- Hive18位身份证号码验证
select * from
(select trim(upper(idcard)) idcard from t) t1
where -- 号码位数不正确
      length(idcard) <> 18
      -- 省份代码不正确
      or substr(idcard,1,2) not in
      ('11','12','13','14','15','21','22','23','31',
       '32','33','34','35','36','37','41','42','43',
       '44','45','46','50','51','52','53','54','61',
       '62','63','64','65','71','81','82','91')
      -- 身份证号码的正则表达式判断
      or (if(pmod(cast(substr(idcard, 7, 4) as int),400) = 0 or
            (pmod(cast(substr(idcard, 7, 4) as int),100) <> 0 and 
             pmod(cast(substr(idcard, 7, 4) as int),4) = 0), -- 闰年
          if(idcard regexp '^[1-9][0-9]{5}19[0-9]{2}((01|03|05|07|08|10|12)(0[1-9]|[1-2][0-9]|3[0-1])|(04|06|09|11)(0[1-9]|[1-2][0-9]|30)|02(0[1-9]|[1-2][0-9]))[0-9]{3}[0-9X]$',1,0),
          if(idcard regexp '^[1-9][0-9]{5}19[0-9]{2}((01|03|05|07|08|10|12)(0[1-9]|[1-2][0-9]|3[0-1])|(04|06|09|11)(0[1-9]|[1-2][0-9]|30)|02(0[1-9]|1[0-9]|2[0-8]))[0-9]{3}[0-9X]$',1,0))) = 0
      -- 校验位不正确
      or substr('10X98765432',pmod(
        (cast(substr(idcard,1,1) as int)+cast(substr(idcard,11,1) as int))*7
       +(cast(substr(idcard,2,1) as int)+cast(substr(idcard,12,1) as int))*9
       +(cast(substr(idcard,3,1) as int)+cast(substr(idcard,13,1) as int))*10
       +(cast(substr(idcard,4,1) as int)+cast(substr(idcard,14,1) as int))*5
       +(cast(substr(idcard,5,1) as int)+cast(substr(idcard,15,1) as int))*8
       +(cast(substr(idcard,6,1) as int)+cast(substr(idcard,16,1) as int))*4
       +(cast(substr(idcard,7,1) as int)+cast(substr(idcard,17,1) as int))*2
       +cast(substr(idcard,8,1) as int)*1
       +cast(substr(idcard,9,1) as int)*6
       +cast(substr(idcard,10,1) as int)*3,11)+1,1)
       <> cast(substr(idcard,18,1) as int);