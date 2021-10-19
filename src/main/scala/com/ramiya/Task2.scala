package com.ramiya
import com.typesafe.config.{Config, ConfigFactory}

import java.lang.Iterable
import java.util.StringTokenizer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.io.Source
import scala.util.matching.Regex
class Task2

/*Task 2: To display the time intervals sorted in the descending order
that contained most log messages of the type ERROR with injected regex pattern string instances.
*/

object Task2 {

  val conf: Config = ConfigFactory.load("application.conf")

  class Task2Mapper1 extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val keyValPattern: Regex = conf.getString("configuration.regexPatternTask2").r
      val inject_pattern : Regex = conf.getString("configuration.injectedStringPattern").r

      val p = keyValPattern.findAllMatchIn(value.toString)
      p.toList.map((pattern) => {
        inject_pattern.findFirstMatchIn(pattern.group(5)) match {
          case Some(_) => {
            word.set(pattern.group(1).split(":")(0))
            context.write(word,one)
          }
          case None => println("error")
        }
      })

    }
  }

  class Task2Reducer1 extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get())
      context.write(key, new IntWritable(sum))
    }
  }

  class Task2Mapper2 extends Mapper[Object, Text, IntWritable, Text] {

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val line = value.toString.split(",")
      val result = line(1).toInt * -1
      context.write(new IntWritable(result), new Text(line(0)))

    }
  }

  class Task2Reducer2 extends Reducer[IntWritable,Text,Text,IntWritable] {
    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      values.asScala.foreach(value => context.write(value, new IntWritable(key.get() * -1)))
    }
  }


//  def main(args: Array[String]): Unit = {
//    val configuration = new Configuration
//    val job = Job.getInstance(configuration,"word count")
//    job.setJarByClass(this.getClass)
//    job.setMapperClass(classOf[Task2Mapper1])
//    job.setCombinerClass(classOf[Task2Reducer1])
//    job.setReducerClass(classOf[Task2Reducer1])
//    job.setOutputKeyClass(classOf[Text])
//    job.setOutputValueClass(classOf[IntWritable]);
//    FileInputFormat.addInputPath(job, new Path(args(0)))
//    FileOutputFormat.setOutputPath(job, new Path(args(1)))
//    if(job.waitForCompletion(true)){
//      val configuration1 = new Configuration
//      val job1 = Job.getInstance(configuration1,"word count")
//      job1.setJarByClass(this.getClass)
//      job1.setMapperClass(classOf[Task2Mapper2])
//      job1.setReducerClass(classOf[Task2Reducer2])
//      job1.setOutputKeyClass(classOf[Text])
//      job1.setOutputValueClass(classOf[IntWritable]);
//      FileInputFormat.addInputPath(job, new Path(args(1)))
//      FileOutputFormat.setOutputPath(job, new Path(args(2)))
//      System.exit(if(job1.waitForCompletion(true))  0 else 1)
//
//    }
//  }

}