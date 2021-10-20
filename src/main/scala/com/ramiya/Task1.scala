package com.ramiya

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}

import java.lang.Iterable
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.StringTokenizer
import scala.collection.JavaConverters._
import scala.util.matching.Regex
class Task1


object Task1 {
  val conf: Config = ConfigFactory.load("application.conf")

  /**This class represents the Mapper Class to find the count of generated log messages with injected regex pattern for each
   message type in a predefined time interval(between the start and end time - both exclusive)
  * @param key : Object - Log Message Tag
   * @param value : Text - Count of the Log Message Tag
   * @return returnType : Unit - Key, Value
   **/

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
                case None => println("The Log message is not matching inject regex pattern")
              }
            })
    }
  }

  /**This class represents the Reducer Class to find the count of generated log messages with injected regex pattern for each
   message type in a predefined time interval(between the start and end time - both exclusive)
   * @param key : Text - Unique Log Message Tag
   * @param value : IntWritable - aggregated count of the Log Message Tag
   * @return returnType : Unit - Key, Value
   **/

  class Task1Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  /**This class represents the Partitioner cLass to partition the data using 2 reduceTasks
   * @param key : Text - Log Message Tag
   * @param value : IntWritable - value 1
   * @param numReduceTasks : Int
   * @return returnType : Int
   **/

  class Task1Partitioner extends Partitioner[Text, IntWritable] {
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {

      if (key.toString == "INFO") {
        return 1 % numReduceTasks
      }
      return 0
    }
  }
}
