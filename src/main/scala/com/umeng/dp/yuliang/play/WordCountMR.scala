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
 * Created by gnailuy on 8/4/15.
 */
object WordCountMR {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration

    val remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    val input = util.Arrays.copyOfRange(remainingArgs, 0, remainingArgs.length - 1)
    val output = remainingArgs(remainingArgs.length - 1)

    val job = Job.getInstance(conf)

    job.setJarByClass(WordCountMR.getClass)
    job.setJobName(WordCountMR.getClass.getSimpleName)

    job.setMapperClass(classOf[WordCountMap])
    job.setCombinerClass(classOf[WordCountReduce])
    job.setReducerClass(classOf[WordCountReduce])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setInputFormatClass(classOf[LzoTextInputFormat])
    job.setOutputFormatClass(classOf[LzoTextOutputFormat[Text, IntWritable]])

    val inputPath: Array[Path] = input.map((p: String) => new Path(p))
    for (p <- inputPath) {
      FileInputFormat.addInputPath(job, p)
    }
    FileOutputFormat.setOutputPath(job, new Path(output))

    job.waitForCompletion(true)
  }
}

class WordCountMap extends Mapper[LongWritable, Text, Text, IntWritable] {
  private val word = new Text
  private val one = new IntWritable(1)

  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    for (w <- value.toString.split(" ")) {
      word.set(w)
      context.write(word, one)
    }
  }
}

class WordCountReduce extends Reducer[Text, IntWritable, Text, IntWritable] {
  private val result = new IntWritable

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    result.set(values.foldLeft(0) {
      _ + _.get()
    })
    context.write(key, result)
  }
}
