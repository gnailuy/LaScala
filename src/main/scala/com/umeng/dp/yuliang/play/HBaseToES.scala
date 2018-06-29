package com.umeng.dp.yuliang.play

import java.text.SimpleDateFormat
import java.util

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, HBaseAdmin}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
import org.apache.spark.serializer.KryoSerializer
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.JavaConversions._

/**
 * Created by adol on 15-9-6.
 */

object HBaseToES {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseToES")
    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    val sc = new SparkContext(sparkConf)

    var username = ""
    var password = ""
    var esnodes = "es10"
    var resource = "app/app"
    var tableName = "app"

    for (arg <- args) {
      if (arg.startsWith("--es-nodes=")) {
        esnodes = arg.substring("--es-nodes=".length)
      } else if (arg.startsWith("--user=")) {
        username = arg.substring("--user=".length)
      } else if (arg.startsWith("--pass=")) {
        password = arg.substring("--pass=".length)
      } else if (arg.startsWith("--resource=")) {
        resource = arg.substring("--resource=".length)
      } else if (arg.startsWith("--table=")) {
        tableName = arg.substring("--table=".length)
      }
    }

    val settings = new util.HashMap[String, String]()
    settings.put(ConfigurationOptions.ES_RESOURCE_WRITE, resource)
    settings.put(ConfigurationOptions.ES_WRITE_OPERATION, ConfigurationOptions.ES_OPERATION_UPSERT)
    settings.put(ConfigurationOptions.ES_MAPPING_ID, "app")
    settings.put(ConfigurationOptions.ES_NODES, esnodes)
    settings.put(ConfigurationOptions.ES_BATCH_WRITE_REFRESH, "false")
    settings.put(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, username)
    settings.put(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, password)

    val conf = HBaseConfiguration.create
    conf.setInt("timeout", 120000)
    conf.set("hbase.master", "hbasemaster:60000")
    conf.set("hbase.zookeeper.quorum", "zk1,zk2,zk3,zk4,zk5")
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val result = hBaseRDD.repartition(32).filter(r => hasAppName(r)).map(r => hbasePairToMap(r))
    result.cache()
    EsSpark.saveToEs(result, settings)
    result.count()
    sc.stop()
  }

  def hasAppName(pair: (ImmutableBytesWritable, Result)): Boolean = {
    pair._2.containsColumn(Bytes.toBytes("app"), Bytes.toBytes("name"))
  }

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val isoDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")

  def hbasePairToMap(pair: (ImmutableBytesWritable, Result)): Map[String, String] = {
    val res = new util.HashMap[String, String]
    val rowkey = pair._1
    pair._2.getNoVersionMap.foreach(kv => {
      val family = Bytes.toString(kv._1)
      kv._2.foreach(qv => {
        val qualifier = Bytes.toString(qv._1)
        var value = Bytes.toString(qv._2)
        if (qualifier.contains("date") || qualifier.equals("created_at")) {
          try {
            value = isoDateFormat.format(dateFormat.parse(value))
          } catch {
            case ex: Exception =>
              println("Date Format Exception")
          }
        }
        res.put(family + ":" + qualifier, value)
      })
    })
    res.put("app", Bytes.toString(rowkey.get()))
    res.toMap
  }
}

