package com.umeng.dp.yuliang.play

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.{MapWritable, Text}

/**
  * Created by gnailuy on 7/13/15.
  */

object SeqReader {
  def main(args: Array[String]) {
    val conf = HBaseConfiguration.create
    val reader = new Reader(conf, Reader.file(new Path(args(0))))
    val k = new Text
    val v = new MapWritable
    while (reader.next(k, v)) {
      println("========")
      println(k.toString.length, k)
      for (k <- v.keySet.toArray) {
        val i = v.get(k)
        println(i.toString.length, i)
      }
    }
    reader.close()
  }
}

