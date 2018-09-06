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


/**
  * Created by admin on 2018/8/10.
  */
object SparkStreamKafka {

  val jedis=DbUtils.getJedis

  val pipeline=DbUtils.getPipeline

  val conn=DbUtils.getHbaseConnection

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

      val keys=jedis.keys("imei:*")

      //业务处理
      rdd.foreachPartition(recored=>{

        val table=conn.getTable(TableName.valueOf("hello"))

        val puts=ListBuffer[Put]()

        val responses =keys.map(x=>{
          (x,pipeline.hgetAll(x))
        }).toMap

        // 一次性发给redis-server
        pipeline.sync()

        recored.foreach(x=>{
          val rowkey=new Date().getTime.toString
          val put=new Put(rowkey.getBytes())

          val vehicleId=responses.get("imei:864287034602894").get.get().get("vehicle_id")
          val value=vehicleId+"_"+x.value()

          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("value"), Bytes.toBytes(value))
          puts.append(put)
          println("============:"+x.value())
        })


        if(puts.toList.size > 0){
          table.put(puts.toList)
        }

        table.close()

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
