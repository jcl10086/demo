package org.jcl.core

import org.apache.spark.sql.SparkSession

/**
  * Created by admin on 2018/7/18.
  */
object WordCount {
  def startJob(master:String): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .master(master)
      .getOrCreate()

    val sc=spark.sparkContext

    val file=sc.textFile("hdfs://node1:8020/1.txt")

    val rdd=file.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(_+_)

    rdd.foreach(println)

    spark.stop()
  }
}
