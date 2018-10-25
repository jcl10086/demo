package org.jcl.core

import redis.clients.jedis.JedisPool

/**
  * Created by admin on 2018/10/25.
  */
object RedisUtils {
  private val host = "app"
  private val port = 6379
  //private val poolConfig = new GenericObjectPoolConfig()
  lazy val pool = new JedisPool(host,port)

  //关闭
  lazy val hooks = new Thread(){
    override def run(): Unit ={
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
}
