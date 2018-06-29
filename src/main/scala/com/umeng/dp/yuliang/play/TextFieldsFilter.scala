package com.umeng.dp.yuliang.play

import java.util

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat
import com.twitter.elephantbird.mapreduce.output.LzoTextOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.GenericOptionsParser

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.collection.mutable.ListBuffer

/**
 * Created by gnailuy on 7/19/16.
 */
object TextFieldsFilter {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration

    val remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs

    var input = ""
    var output = ""
    var outputFields = ""
    var filters = ""

    for (arg <- remainingArgs) {
      if (arg.startsWith("--input=")) {
        input = arg.substring("--input=".length)
      } else if (arg.startsWith("--output=")) {
        output = arg.substring("--output=".length)
      } else if (arg.startsWith("--output-fields=")) {
        outputFields = arg.substring("--output-fields=".length)
      } else if (arg.startsWith("--filter=")) {
        filters = arg.substring("--filter=".length)
      }
    }

    if ("".equals(filters)) {
      println("--filter=field:op:value,field:op:value cannot be empty!")
      return
    }

    conf.set("filters", filters)
    conf.set("output.fields", outputFields)
    val job = Job.getInstance(conf)

    job.setJarByClass(TextFieldsFilter.getClass)
    job.setJobName(TextFieldsFilter.getClass.getSimpleName + input)

    job.setMapperClass(classOf[TextFieldsFilterMapper])
    job.setReducerClass(classOf[TextFieldsFilterReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[NullWritable])

    job.setInputFormatClass(classOf[LzoTextInputFormat])
    job.setOutputFormatClass(classOf[LzoTextOutputFormat[Text, NullWritable]])

    FileInputFormat.addInputPath(job, new Path(input))
    println("Added Input: " + input)
    FileOutputFormat.setOutputPath(job, new Path(output))
    println("Set Output: " + output)

    job.waitForCompletion(true)
  }
}

class TextFieldsFilterMapper extends Mapper[LongWritable, Text, Text, NullWritable] {
  private val filterRules = new util.LinkedList[(Int, (String, String) => Boolean, String)]
  private val outputFields = new util.LinkedList[Int]

  def greatThan(first: String, second: String): Boolean = {
    if (first.compareTo(second) > 0) true else false
  }

  def lessThan(first: String, second: String): Boolean = {
    if (first.compareTo(second) < 0) true else false
  }

  def equalTo(first: String, second: String): Boolean = {
    if (first.equals(second)) true else false
  }

  def notEqualTo(first: String, second: String): Boolean = {
    if (!first.equals(second)) true else false
  }

  private val opMap = immutable.HashMap[String, (String, String) => Boolean](
    "gt" -> greatThan,
    "lt" -> lessThan,
    "eq" -> equalTo,
    "ne" -> notEqualTo
  )

  override def setup(context: Mapper[LongWritable, Text, Text, NullWritable]#Context): Unit = {
    val filters = context.getConfiguration.get("filters").split(",")
    for (filter <- filters) {
      val parts = filter.split(":", -1)
      if (parts.length == 3 && opMap.containsKey(parts(1))) {
        filterRules.add((parts(0).toInt, opMap(parts(1)), parts(2)))
      } else {
        println("Invalid rule!")
      }
    }
    val outputFieldList = context.getConfiguration.get("output.fields").split(",")
    for (field <- outputFieldList) {
      outputFields.add(field.toInt)
    }
  }

  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, Text, NullWritable]#Context): Unit = {
    val parts = value.toString.split("\t", -1)
    var result = true
    for (rule <- filterRules) {
      if (parts.length >= rule._1) {
        result = result && rule._2(parts(rule._1), rule._3)
      }
      if (!result) return
    }
    context.getCounter("APP", "Valid Result").increment(1)
    if (outputFields.isEmpty) {
      context.write(value, NullWritable.get())
    } else {
      val o = new ListBuffer[String]
      for (idx <- outputFields) {
        o += parts(idx)
      }
      context.write(new Text(o.mkString("\t")), NullWritable.get())
    }
  }
}

class TextFieldsFilterReducer extends Reducer[Text, NullWritable, Text, NullWritable] {

  override def reduce(key: Text, values: java.lang.Iterable[NullWritable],
                      context: Reducer[Text, NullWritable, Text, NullWritable]#Context): Unit = {
    context.write(key, NullWritable.get())
  }
}

