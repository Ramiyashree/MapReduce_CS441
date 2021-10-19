package com.ramiya

import com.typesafe.config.{Config, ConfigFactory}
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

/*Task 1: To find the count of generated log messages with injected regex pattern for each
message type in a predefined time interval.
 */

object Task1 {
  val conf: Config = ConfigFactory.load("application.conf")

  class Task1Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object,
                     value: Text,
                     context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val keyValPattern: Regex = conf.getString("configuration.regexPattern").r
      val inject_pattern : Regex = conf.getString("configuration.injectedStringPattern").r

      val formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
      val startTime = LocalTime.parse(conf.getString("configuration.startTime"), formatter)
      val endTime = LocalTime.parse(conf.getString("configuration.endTime"), formatter)

      val patternMatch =  keyValPattern.findFirstMatchIn(value.toString)
            patternMatch.toList.map(x => {
              inject_pattern.findFirstMatchIn(x.group(5)) match {
                case Some(_) => {
                  val time = LocalTime.parse(x.group(1), formatter)
                  if(startTime.isBefore(time) && endTime.isAfter(time)){
                    word.set(x.group(3))
                    context.write(word,one)
                  }
                }
                case None => println("Injected Regex Pattern is not matching the log message")
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
}
