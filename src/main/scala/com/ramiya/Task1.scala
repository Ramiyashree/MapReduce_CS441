package com.ramiya

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.StringTokenizer
import scala.collection.JavaConverters._
import scala.util.matching.Regex
class Task1

object Task1 {
  class Task1Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object,
                     value: Text,
                     context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val keyValPattern: Regex = "(^\\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s\\[([^\\]]*)\\]\\s(WARN|INFO|DEBUG|ERROR)\\s+([A-Z][A-Za-z\\.]+)\\$\\s-\\s(.*)".r
      val formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
      val inject_pattern : Regex = "[\\w]+".r
      val startTime = LocalTime.parse("13:02:53.641", formatter)
      val endTime = LocalTime.parse("13:02:53.767", formatter)

      val patternMatch =  keyValPattern.findFirstMatchIn(value.toString)
            patternMatch.toList.map(x => {
              inject_pattern.findFirstMatchIn(x.group(5)) match {
                case Some(_) => {
                  val time = LocalTime.parse(x.group(1), formatter)
                  if(startTime.isBefore(time) && endTime.isAfter(time)){
                    inject_pattern.findFirstMatchIn(x.group(5))
                    word.set(x.group(3))
                    context.write(word,one)
                  }
                }
                case None => println("jgftfy")
              }
            })
    }
  }

  class Task1Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

//  def main(args: Array[String]): Unit = {
//    val configuration = new Configuration()
//    val job = Job.getInstance(configuration, "word count")
//    job.setJarByClass(this.getClass)
//    job.setMapperClass    (classOf[Task1Mapper])
//    job.setCombinerClass(classOf[Task1Reducer])
//    job.setReducerClass(classOf[Task1Reducer])
//    job.setOutputKeyClass(classOf[Text])
//    job.setOutputValueClass(classOf[IntWritable])
//    FileInputFormat.addInputPath(job, new Path(args(0)))
//    FileOutputFormat.setOutputPath(job, new Path(args(1)))
//    System.exit(if(job.waitForCompletion(true)) 0 else 1)
//  }
}
