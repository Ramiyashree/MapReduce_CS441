package com.ramiya

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}

import java.lang.Iterable
import java.util.StringTokenizer
import scala.collection.JavaConverters._
import scala.util.matching.Regex
class Task4

object Task4 {
  val conf: Config = ConfigFactory.load("application.conf")

  /**
  This class represents the Mapper class to produce the number of characters in each log message for each log message type that contain the
  highest number of characters in the detected instances of the designated regex pattern.
   * @param key : Object - Log Message Tag
   * @param value : Text - value 1
   * @return returnType : Unit - [Key, Value]
   **/

  class Task4Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val logTag = new Text()

    override def map(key: Object,
                     value: Text,
                     context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val keyValPattern: Regex = conf.getString("configuration.regexPattern").r
      val inject_pattern : Regex = conf.getString("configuration.injectedStringPattern").r

      // If the a Log entry matches the regex pattern, the generated log messages matches the injected string pattern,
      // every log message and its count is passed to reducer

      val patternMatch =  keyValPattern.findFirstMatchIn(value.toString)
      patternMatch.toList.map((pattern) => {
        inject_pattern.findFirstMatchIn(pattern.group(5)) match {
          case Some(_) => {
            val charlength =new IntWritable(pattern.group(5).length)
            logTag.set(pattern.group(3))
            context.write(logTag, charlength)
          }
          case None => println("error")
        }
      })
    }
  }

  /**
  This class represents the Reducer class to produce the number of characters in each log message for each log message type that contain the
  highest number of characters in the detected instances of the designated regex pattern.
   * @param key : Text - Log Message Tag
   * @param value : IntWritable - max value of every log message tag
   * @return returnType : Unit - (Key, Value)
   **/

  class Task4Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      // the max of the value for a specific log message tag is retrieved
      val sum = values.asScala.foldLeft(0)(_ max _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  /**This class represents the Partitioner cLass to partition the data using 2 reduceTasks
   * @param key : Text - Log Message Tag
   * @param value : IntWritable - value 1
   * @param numReduceTasks : Int
   * @return returnType : Int
   **/

  class Task4Partitioner extends Partitioner[Text, IntWritable] {
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {
      if (key.toString == "INFO") {
        return 1 % numReduceTasks
      }
      return 0
    }
  }

}
