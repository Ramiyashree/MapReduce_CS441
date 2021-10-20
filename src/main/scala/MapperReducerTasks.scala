import com.ramiya.{Task1, Task2, Task3, Task4}
import com.ramiya.Task1.{Task1Mapper, Task1Reducer, Task1Partitioner}
import com.ramiya.Task2.{Task2Mapper1, Task2Mapper2, Task2Reducer1, Task2Reducer2, Task2Partitioner}
import com.ramiya.Task3.{Task3Mapper, Task3Reducer, Task3Partitioner}
import com.ramiya.Task4.{Task4Mapper, Task4Reducer, Task4Partitioner}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.slf4j.{Logger, LoggerFactory}

class MapperReducerTasks

object MapperReducerTasks{
val logger: Logger = LoggerFactory.getLogger(getClass)

val conf: Config = ConfigFactory.load("application.conf")

val inputFile: String = conf.getString("configuration.inputFile")
val outputFile: String = conf.getString("configuration.outputFile")

def main(args: Array[String]): Unit = {
  val startTime = System.nanoTime
  logger.info("--- Starting AllMapReduce ---")


  val configure: Configuration = new Configuration()

  //Format as CSV output
  configure.set("mapred.textoutputformat.separator", ",")

  val job1Name = conf.getString("configuration.job1")
  val job2Name = conf.getString("configuration.job2")
  val job2NameMR2 = conf.getString("configuration.job2MR2")
  val job3Name = conf.getString("configuration.job3")
  val job4Name = conf.getString("configuration.job4")

   /**Task 1: To find the count of generated log messages with injected regex pattern for each
   message type in a predefined time interval.
   **/

  val job1: Job = Job.getInstance(configure, job1Name)
  job1.setJarByClass(classOf[Task1])
  job1.setMapperClass(classOf[Task1Mapper])
  job1.setCombinerClass(classOf[Task1Reducer])
  job1.setPartitionerClass(classOf[Task1Partitioner])
  job1.setNumReduceTasks(2)
  job1.setReducerClass(classOf[Task1Reducer])
  job1.setOutputKeyClass(classOf[Text]);
  job1.setOutputValueClass(classOf[IntWritable]);
  job1.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
  FileInputFormat.addInputPath(job1, new Path(inputFile))
  FileOutputFormat.setOutputPath(job1, new Path((outputFile + "/" + job1Name)))
  job1.waitForCompletion(true)


  /**Task 2: To display the time intervals sorted in the descending order
  that contained most log messages of the type ERROR with injected regex pattern string instances.
  **/

  val job2: Job = Job.getInstance(configure, job2Name)
  job2.setJarByClass(classOf[Task2])
  job2.setMapperClass(classOf[Task2Mapper1])
  job2.setCombinerClass(classOf[Task2Reducer1])
  job2.setPartitionerClass(classOf[Task2Partitioner])
  job2.setNumReduceTasks(2)
  job2.setReducerClass(classOf[Task2Reducer1])
  job2.setOutputKeyClass(classOf[Text])
  job2.setOutputValueClass(classOf[IntWritable])
  FileInputFormat.addInputPath(job2, new Path(inputFile))
  FileOutputFormat.setOutputPath(job2, new Path((outputFile + "/" + job2Name)))
  job2.waitForCompletion(true)
  val configuration1 = new Configuration
  val job2a = Job.getInstance(configuration1,"word count")
  job2a.setJarByClass(classOf[Task2])
  job2a.setMapperClass(classOf[Task2Mapper2])
  job2a.setReducerClass(classOf[Task2Reducer2])
  job2a.setMapOutputKeyClass(classOf[IntWritable])
  job2a.setMapOutputValueClass(classOf[Text])
  job2a.setOutputKeyClass(classOf[Text])
  job2a.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(job2a, new Path(outputFile + "/" + job2Name))
  FileOutputFormat.setOutputPath(job2a, new Path(outputFile + "/" + job2NameMR2))
  job2a.waitForCompletion(true)

  /**Task 3: To find the count of generated log messages for each message type.
  **/

  val job3: Job = Job.getInstance(configure, job3Name)
  job3.setJarByClass(classOf[Task3])
  job3.setMapperClass(classOf[Task3Mapper])
  job3.setCombinerClass(classOf[Task3Reducer])
  job3.setPartitionerClass(classOf[Task3Partitioner])
  job3.setNumReduceTasks(2)
  job3.setReducerClass(classOf[Task3Reducer])
  job3.setOutputKeyClass(classOf[Text])
  job3.setOutputValueClass(classOf[IntWritable])
  job3.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
  FileInputFormat.addInputPath(job3, new Path(inputFile))
  FileOutputFormat.setOutputPath(job3, new Path((outputFile + "/" + job3Name)))
  job3.waitForCompletion(true)

  /**
  Task 4: To produce the number of characters in each log message for each log message type that contain the
  highest number of characters in the detected instances of the designated regex pattern.
   **/

  val job4: Job = Job.getInstance(configure, job4Name)
  job4.setJarByClass(classOf[Task4])
  job4.setMapperClass(classOf[Task4Mapper])
  job4.setCombinerClass(classOf[Task4Reducer])
  job4.setPartitionerClass(classOf[Task4Partitioner])
  job4.setNumReduceTasks(2)
  job4.setReducerClass(classOf[Task4Reducer])
  job4.setOutputKeyClass(classOf[Text])
  job4.setOutputValueClass(classOf[IntWritable])
  job4.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
  FileInputFormat.addInputPath(job4, new Path(inputFile))
  FileOutputFormat.setOutputPath(job4, new Path((outputFile + "/" + job4Name)))
  job4.waitForCompletion(true)
//
//  if(job1.waitForCompletion(true)){
//    logger.info("Task1 executed successfully")
//  }else{
//    logger.info("Task1 failed")
//  }
//
//  if(job2.waitForCompletion(true)){
//    logger.info("Task2 executed successfully")
//  }else{
//    logger.info("Task2 failed")
//  }
//
//  if(job2a.waitForCompletion(true)){
//    logger.info("Task2 Sorting executed successfully")
//  }else{
//    logger.info("Task2 Sorting failed")
//  }
//
//  if(job3.waitForCompletion(true)){
//    logger.info("Task3 executed successfully")
//  }else{
//    logger.info("Task3 failed")
//  }
//
//  if(job4.waitForCompletion(true)){
//    logger.info("Task4 executed successfully")
//  }else{
//    logger.info("Task4 failed")
//  }
//
//
//  if (job1.waitForCompletion(true) && job2.waitForCompletion(true) && job2a.waitForCompletion(verbose) && job3.waitForCompletion(verbose) && job4.waitForCompletion(verbose)) {
//    val endTime = System.nanoTime
//    val totalTime = endTime - startTime
//    logger.info("--- SUCCESSFULLY COMPLETED (Execution completed in: " + totalTime / 1_000_000_000 + " sec) ---")
//  } else {
//    logger.info("--- UNFORTUNATELY FAILED ---")
//  }
}
}