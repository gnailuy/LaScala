package com.umeng.dp.yuliang.play

import java.util

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat
import com.twitter.elephantbird.mapreduce.output.LzoTextOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, IntWritable, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Reducer, Mapper, Job}
import org.apache.hadoop.util.GenericOptionsParser
import scala.collection.JavaConversions._

/**
 * Created by gnailuy on 10/30/15.
 */
object KeyCountMR {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration

    val remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    val input = util.Arrays.copyOfRange(remainingArgs, 0, remainingArgs.length - 1)
    val output = remainingArgs(remainingArgs.length - 1)

    val job = Job.getInstance(conf)

    job.setJarByClass(KeyCountMR.getClass)
    job.setJobName("KeyCountMR")

    job.setMapperClass(classOf[KeyCountMap])
    job.setReducerClass(classOf[KeyCountReduce])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job.setInputFormatClass(classOf[LzoTextInputFormat])
    job.setOutputFormatClass(classOf[LzoTextOutputFormat[Text, IntWritable]])

    val inputPath: Array[Path] = input.map((p: String) => new Path(p))
    for (p <- inputPath) {
      FileInputFormat.addInputPath(job, p)
      println("Added input " + p)
    }
    FileOutputFormat.setOutputPath(job, new Path(output))
    println("Added output " + output)

    job.waitForCompletion(true)
  }
}

class KeyCountMap extends Mapper[LongWritable, Text, Text, IntWritable] {
  private val keyword = new Text
  private val count = new IntWritable

  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val parts = value.toString.split("\t")
    if (2 != parts.length) {
      context.getCounter("APP", "Error Line").increment(1)
      return
    }
    keyword.set(parts(0))
    count.set(Integer.valueOf(parts(1)))
    context.write(keyword, count)
  }
}

class KeyCountReduce extends Reducer[Text, IntWritable, Text, IntWritable] {
  private val result = new IntWritable

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    result.set(values.foldLeft(0) {
      _ + _.get()
    })
    if (1 == result.get()) {
      context.getCounter("APP", "Not Duplicate").increment(1)
    } else {
      context.getCounter("APP", "Duplicate Key").increment(1)
      context.write(key, result)
    }
  }
}

