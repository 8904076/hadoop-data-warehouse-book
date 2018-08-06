# 第6章
## 配置Hive支持事务：
### (1) 添加支持事务的属性
```
vi /etc/hive/conf.cloudera.hive/hive-site.xml
<!-- 添加如下6个属性以支持事务 -->
<property>
<name>hive.support.concurrency</name>
<value>true</value>
</property>
<property>
<name>hive.exec.dynamic.partition.mode</name>
<value>nonstrict</value>
</property>
<property>
<name>hive.txn.manager</name>
<value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
</property>
<property>
<name>hive.compactor.initiator.on</name>
<value>true</value>
</property>
<property>
<name>hive.compactor.worker.threads</name>
<value>1</value>
</property>
<property>
<name>hive.enforce.bucketing</name>
<value>true</value>
</property>
```

### (2) 在MySQL中添加Hive元数据
```
mysql -uroot -phive
mysql> use hive;
mysql> insert into NEXT_LOCK_ID values (1);
mysql> insert into NEXT_COMPACTION_QUEUE_ID values (1);
mysql> insert into NEXT_TXN_ID values (1);
mysql> commit;
```

### beeline连接hiveserver2
```
!connect jdbc:hive2://node4.bupt.edu.cn:10000
用户名zzy
```

#### 可以通过`DESCRIBE FORMATTED tablename`语句查看hive表是管理表还是外部表。
```
Table Type: MANAGED_TABLE
Table Type: EXTERNAL_TABLE
```

#### 创建一张结构相同，不包含数据的表
```
create external table if not exists mydb.empty_key_value_store
like mydb.key_value_store
location '/path/to/data';
```

#### 分区表
```
create table page_view(viewtime int, userid bigint, page_url string,
referrer_url string, ip string comment 'ip address of the user')
comment 'this is the page view table'
partitioned by (dt string, country string)
row format delimited fields terminated by '\001'
stored as sequencefile;
```
**注意**：创建分区表时，普通字段与分区字段不能重名

##### 设置分区查询为严格模式，防止提交一个宽范围的查询
```
hive> set hive.mapred.mode=strict;
```
恢复默认：
```
hive> set hive.mapred.mode=nonstrict;
```

#### 向分区表中装载数据
```
create table t1 (name string) partitioned by (country string,state string);
load data inpath '/user/test' into table t1 partition (country='us', state='ca');

select * from t1;
```

#### 增加分区
```
alter table t1 add partition(country='us',state='cb')
location '/a';
```

#### 动态分区插入
**注意**：
- 1. OVERWRITE不会删除已有的分区目录，只会追加新分区，并覆盖已有分区的非分区数据
- 2. 不能使用LOAD进行动态分区插入

# 第8章
### 在命令行中执行HiveQL语句
```
beeline -u jdbc:hive2://node5.bupt.edu.cn:10000/test -e "select * from t"
```

### 执行HiveQL文件
```
beeline -u jdbc:hive2://node5.bupt.edu.cn:10000/dw -f create_table_date_dim.hql
```

### 操作HDFS
```
dfs -ls /;
dfs -help;
```

### 查看函数帮助信息
```
show functions;
desc function abs;
desc function extended abs;
```