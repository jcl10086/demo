1.HbaseSpark  Spark与HBase的整合  启动参数需要指定hbase-site.xml 来覆盖默认参数   --files /etc/hbase/conf/hbase-site.xml
例如：
大表查询需要指定 scan超时时间

    <property>
        <name>hbase.client.scanner.timeout.period</name>
        <value>600000</value>
    </property>

    <property>
        <name>hbase.rpc.timeout</name>
        <value>180000</value>
    </property>

2.spark streaming 整合kafka    同通过引入spark-streaming-kafka-0-10_2.11      

手动维护偏移量，将偏移量存储在redis里

![image](https://github.com/jcl10086/demo/blob/master/src/main/resources/redis.png)

数据批量插入hbase  spark-redis整合优雅的使用redis


3.hbase预创建regoin并制定SNAPPY压缩
