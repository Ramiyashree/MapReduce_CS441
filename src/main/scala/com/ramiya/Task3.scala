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
class Task3

object Task3 {

  val conf: Config = ConfigFactory.load("application.conf")

  /**This class represents the Mapper class to find the count of generated log messages for each message type.
   **/

  class Task3Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object,
                     value: Text,
                     context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val keyValPattern: Regex = conf.getString("configuration.regexPattern").r

            val p = keyValPattern.findAllMatchIn(value.toString)
            p.toList.map((pattern) => {
              word.set(pattern.group(3))
              context.write(word,one)
            })
    }
  }

  /**This class represents the Reducer class to find the count of generated log messages for each message type.
   **/

  class Task3Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  /**This class represents the Partitioner cLass to partition the data using 2 reduceTasks
   **/

  class Task3Partitioner extends Partitioner[Text, IntWritable] {
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {

      if (key.toString == "INFO") {
        return 1 % numReduceTasks
      }
      return 0
    }
  }

}
