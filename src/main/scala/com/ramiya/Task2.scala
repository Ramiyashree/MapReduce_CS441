package com.ramiya
import com.typesafe.config.{Config, ConfigFactory}

import java.lang.Iterable
import java.util.StringTokenizer
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}

import scala.io.Source
import scala.util.matching.Regex
class Task2


object Task2 {

  val conf: Config = ConfigFactory.load("application.conf")

  /**Task 2: This class represents the Mapper cLass to display the count log messages of the type ERROR with injected regex pattern string instances in every one hour(time intervals).
   * @param key : Object - Time Interval
   * @param value : Text -  message count
   * @return returnType : Unit - (key, value)
   **/

  class Task2Mapper1 extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val timeInterval = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {


      val keyValPattern: Regex = conf.getString("configuration.regexPatternTask2").r
      val inject_pattern : Regex = conf.getString("configuration.injectedStringPattern").r

      // If the a Log entry matches the regex pattern, and the generated log messages matches the injected string pattern
      // the log count for every hour is passed to the reducer

      val p = keyValPattern.findAllMatchIn(value.toString)
      p.toList.map((pattern) => {
        inject_pattern.findFirstMatchIn(pattern.group(5)) match {
          case Some(_) => {
            timeInterval.set(pattern.group(1).split(":")(0))
            context.write(timeInterval,one)
          }
          case None => println("The Log message is not matching inject regex pattern")
        }
      })

    }
  }


  /**Task 2: This class represents the Reducer cLass to display the count log messages of the type ERROR with injected regex pattern string instances in every one hour(time intervals).
   * @param key : Text - Time Interval
   * @param value : IntWritable - Aggregated Message count
   * @return returnType : Unit - (key, value)
   **/

  class Task2Reducer1 extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get())
      context.write(key, new IntWritable(sum))
    }
  }


  /**This class represents the Partitioner cLass to partition the data using 2 reduceTasks
   * @param key : Text - Time Interval
   * @param value : IntWritable - value 1
   * @param numReduceTasks : Int
   * @return returnType : Int
   **/

  class Task2Partitioner extends Partitioner[Text, IntWritable] {
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {
      if (key.toString.toInt >= 1 && key.toString.toInt <= 12) {
        return 1 % numReduceTasks
      }
      return 0
    }
  }

  /**Task 2: This class represents the Mapper cLass to display in SORTED ORDER based on the count log messages of the type ERROR with injected regex pattern string instances in every one hour(time intervals)
   * @param key : Object - Time Interval
   * @param value : Text -  Aggregated Message Count
   * @return returnType : Unit - (key, value)
   **/

  class Task2Mapper2 extends Mapper[Object, Text, IntWritable, Text] {

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {

      // Here, the count of the log messages is multiplied with -1 and passed as key which will be sorted in descending order
      val line = value.toString.split(",")
      val result = line(1).toInt * -1
      context.write(new IntWritable(result), new Text(line(0)))

    }
  }

  /**Task 2: This class represents the Reducer cLass to display in SORTED ORDER based on the count log messages of the type ERROR with injected regex pattern string instances in every one hour(time intervals)
   * @param key : IntWritable - Time Interval
   * @param value : Text - Sorted order count(descending order)
   * @return returnType : Unit - (key, value)
   **/

  class Task2Reducer2 extends Reducer[IntWritable,Text,Text,IntWritable] {
    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      values.asScala.foreach(value => context.write(value, new IntWritable(key.get() * -1)))
    }
  }

}