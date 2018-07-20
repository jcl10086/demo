package org.jcl.core

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.SparkSession

/**
  * Created by admin on 2018/7/20.
  * local:ccu_data local
  * yarn:ccu_data yarn
  */
object HbaseTest {
  def main(args: Array[String]): Unit = {

    val table=args(0)

    val master=args(1)

    val spark = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .master(master)
      .getOrCreate()

    val conf=HBaseConfiguration.create()

    //设置读取的表
    conf.set(TableInputFormat.INPUT_TABLE,table)

    val sc=spark.sparkContext

    val scan = new Scan()
    scan.setStartRow("362387494f6be6613daea643a7706a42_20171227".getBytes())
    scan.setStopRow("362387494f6be6613daea643a7706a42_20171228".getBytes())
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hbaseRDD.map(x => {
      val createAt=x._2.getValue("base_info".getBytes(),"createAt".getBytes())
      Bytes.toString(createAt)
    }).foreach(println)

  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

}
