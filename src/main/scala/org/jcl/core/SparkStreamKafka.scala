package org.jcl.core

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.jcl.util.DbUtils

import scala.collection.JavaConversions._


/**
  * Created by admin on 2018/8/10.
  */
object SparkStreamKafka {

  val jedis=DbUtils.getJedis

  def startJob(master:String): Unit ={

    val spark = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .master(master)
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

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

    val data=jedis.hgetAll(key)

    //从redis里获取偏移量
    val fromOffsets: Map[TopicPartition, Long] = data.map { x =>
      val partition=x._1
      val offset=x._2
      new TopicPartition(topic, partition.toInt) -> offset.toLong
    }.toMap

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
      //获取当前偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //业务处理
      rdd.foreachPartition(recored=>{
        recored.foreach(x=>{
          println("============:"+x.value())
        })
      })

      //偏移量存储redis
      for(of <- offsetRanges){
        val topic=of.topic
        val partition=of.partition.toString
        val offset=of.untilOffset.toString

        val key="gt:"+groupId+"_"+topic

        jedis.hset(key,partition,offset)
      }
    })

    //开启计算
    ssc.start()
    ssc.awaitTermination()
  }
}
