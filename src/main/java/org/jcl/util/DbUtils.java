package org.jcl.util;/**
 * Created by admin on 2018/9/5.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import redis.clients.jedis.Jedis;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * @author jichenglu
 * @create 2018-09-05 11:03
 **/
public class DbUtils {

    public static Jedis getJedis(){
        Jedis jedis = new Jedis("app", 6379);
        jedis.auth("touchspring");
        return jedis;
    }

    public static Properties getProps() throws Exception{
        Properties props = new Properties();
        //本地 data/config.properties
        props.load(new FileInputStream("data/config.properties"));
        return props;
    }

    /**
     * hbase连接
     * @return
     */
    public static Connection getHbaseConnection(){
        Connection connection=null;
        try {
            Properties props=getProps();
            String port=props.get("hbase.zookeeper.property.clientPort").toString();
            String quorum=props.get("hbase.zookeeper.quorum").toString();

            Configuration conf= HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", quorum);
            conf.set("hbase.zookeeper.property.clientPort", port);
            connection= ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
        return connection;
    }


}
