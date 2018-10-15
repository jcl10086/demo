package org.jcl.core

import java.util.Date

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.jcl.util.DbUtils

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import com.redislabs.provider.redis._


/**
  * Created by admin on 2018/8/10.
  */
object SparkStreamKafka {

//  val jedis=DbUtils.getJedis
//
//  val pipeline=DbUtils.getPipeline

  val conn=DbUtils.getHbaseConnection

  def startJob(master:String): Unit ={

    val spark = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .master(master)
      .config("redis.host", "app")
      .config("redis.port", "6379")
      .config("redis.auth", "touchspring")
      .config("redis.db","0")
      .getOrCreate()

    val sc=spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(2))

    val groupId="moto"

    val topic="hello"

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)

    val key="gt:"+groupId+"_"+topic

    val dataRDD=sc.fromRedisHash(key)

//    val data=jedis.hgetAll(key)

    //从redis里获取偏移量
    val fromOffsets: Map[TopicPartition, Long] = dataRDD.map { x =>
      val partition=x._1
      val offset=x._2
      new TopicPartition(topic, partition.toInt) -> offset.toLong
    }.collect().toMap

    val stream = if (fromOffsets.size==0) {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
    }else{
      KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent,
        ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets)
      )
    }

    stream.foreachRDD(rdd=>{

      //一次性查出所有值
      val keys=sc.fromRedisKeyPattern("imei:*").collect().toList

      val mapData=keys.map(key=>{
        (key,sc.fromRedisHash(key).collect().toMap)
      }).toMap


      //获取当前偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //业务处理
      rdd.foreachPartition(recored=>{

        val table=conn.getTable(TableName.valueOf("hello"))

        val puts=ListBuffer[Put]()

        recored.foreach(x=>{
          val rowkey=new Date().getTime.toString
          val put=new Put(rowkey.getBytes())

          val vehicleId=mapData.get("imei:867154030090366").get.get("vehicle_id").get
          val value=vehicleId+"_"+x.value()

          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("value"), Bytes.toBytes(value))
          puts.append(put)
          println("============:"+x.value())
        })


        if(puts.toList.size > 0){
          table.put(puts.toList)
        }
       
//         table.close()

      })

      val list=ListBuffer[(String,String)]()


      val key="gt:"+groupId+"_"+topic

      //偏移量存储redis
      for(of <- offsetRanges){
//        val topic=of.topic
        val partition=of.partition.toString
        val offset=of.untilOffset.toString
        list.append((partition,offset))
//        jedis.hset(key,partition,offset)
      }
      val rsRDD=sc.parallelize(list)
      sc.toRedisHASH(rsRDD,key)

    })

    //开启计算
    ssc.start()
    ssc.awaitTermination()
  }
}
