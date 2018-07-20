package org.jcl.core

import org.apache.spark.sql.SparkSession

/**
  * Created by admin on 2018/7/20.
  * hive外部表关联hbase  spark sql查询hbase
  * 本地:local
  * 集群:yarn
  */
object SparkSqlHive {

  def startJob(master:String,st:String,et:String): Unit = {

//    val master=args(0)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql


    val startTime = System.currentTimeMillis()

    sql("select count(*) from ccu_data where value['createAt'] > "+ st +" and value['createAt'] < "+ et +"").show()

    val endTime = System.currentTimeMillis()
    println("action cost time:" + ((endTime - startTime)/1000.0) + "s")

    spark.stop()
  }
}
