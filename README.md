1.HbaseSpark  Spark与HBase的整合  启动参数需要指定hbase-site.xml 来覆盖默认参数   --files /etc/hbase/conf/hbase-site.xml
例如：
大表查询需要指定 scan超时时间
<property>
        <name>hbase.client.scanner.timeout.period</name>
        <value>600000</value>
 </property>

