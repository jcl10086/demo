package org.jcl.core

import java.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * Created by admin on 2018/7/17.
  * 本地：ccu_data local "2018-01-01 00:00:00" "2018-01-02 00:00:00"
  * 集群：ccu_data yarn "2018-01-01 00:00:00" "2018-01-02 00:00:00"
  *
  */
object HbaseSpark {

  def main(args: Array[String]): Unit = {

    val table=args(0)

    val master=args(1)

    val st=args(2)

    val et=args(3)

    val cat=initCat(table)

    val spark = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .master(master)
      .getOrCreate()

    val sqlContext=spark.sqlContext

    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    val df=withCatalog(cat)
//    df.persist(StorageLevel.MEMORY_ONLY)
//    df.show()


    val startTime = System.currentTimeMillis()
    //    val count=df.filter($"rowkey" < "1f2cb739c37d19f9d6c0230591f17eda_20160709000000_466_376").count()
    val count=df.filter($"createAt" > st && $"createAt" < et).count()

    //    val count=df.filter($"rowkey" like("%_20180101%")).count()

    println("数量=========="+count)
    val endTime = System.currentTimeMillis()
    println("action cost time:" + ((endTime - startTime)/1000.0) + "s")

    spark.stop()

  }


  def initCat(table:String): String ={

    val listBuffer=ListBuffer[String]()

    listBuffer.append("name")
    listBuffer.append("createAt")
    listBuffer.append("imei")
    listBuffer.append("taskId")
    listBuffer.append("confId")

    var str=""
    for(s <- listBuffer){
      str += "\"" + s +"\":{\"cf\":\"base_info\", \"col\":\"" + s + "\", \"type\":\"string\"},"
    }
    str=str.substring(0,str.length-1)

    val c=s"""{
             |"table":{"namespace":"default", "name":"${table}"},
             |"rowkey":"rowkey",
             |"columns":{
             |"rowkey":{"cf":"rowkey", "col":"rowkey", "type":"string"},
             |${str}
             |}
             |}""".stripMargin

    c
  }



}
