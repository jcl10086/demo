package org.jcl.util;/**
 * Created by admin on 2018/9/5.
 */

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
}
