package org.jcl.util;/**
 * Created by admin on 2018/9/5.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import redis.clients.jedis.Jedis;

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

    /**
     * hbase连接
     * @return
     */
    public static Connection getHbaseConnection(){
        Connection connection=null;
        try {
            Configuration conf= HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "node1");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection= ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
        return connection;
    }
}
