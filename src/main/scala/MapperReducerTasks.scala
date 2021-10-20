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

  // THE INPUT DIRECTORY AND OUTPUT IS FIXED HERE
val inputFile: String = conf.getString("configuration.inputFile")
val outputFile: String = conf.getString("configuration.outputFile")

def main(args: Array[String]): Unit = {

  logger.info("Starting MapReduce for all tasks")

  val configure: Configuration = new Configuration()
  val configuration1: Configuration = new Configuration()


  //Format as CSV output
  configure.set("mapred.textoutputformat.separator", ",")
  configuration1.set("mapred.textoutputformat.separator", ",")

  val task1Name = conf.getString("configuration.task1")
  val task2Name = conf.getString("configuration.task2")
  val task2NameMR2 = conf.getString("configuration.task2MR2")
  val task3Name = conf.getString("configuration.task3")
  val task4Name = conf.getString("configuration.task4")

   /**Task 1: To find the count of generated log messages with injected regex pattern for each
   message type in a predefined time interval.
   **/

  val job1: Job = Job.getInstance(configure, task1Name)
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
  FileOutputFormat.setOutputPath(job1, new Path((outputFile + "/" + task1Name)))

  /**Task 2: To display the time intervals sorted in the descending order
  that contained most log messages of the type ERROR with injected regex pattern string instances.
  **/

  val job2: Job = Job.getInstance(configure, task2Name)
  job2.setJarByClass(classOf[Task2])
  job2.setMapperClass(classOf[Task2Mapper1])
  job2.setCombinerClass(classOf[Task2Reducer1])
  job2.setPartitionerClass(classOf[Task2Partitioner])
  job2.setNumReduceTasks(2)
  job2.setReducerClass(classOf[Task2Reducer1])
  job2.setOutputKeyClass(classOf[Text])
  job2.setOutputValueClass(classOf[IntWritable])
  FileInputFormat.addInputPath(job2, new Path(inputFile))
  FileOutputFormat.setOutputPath(job2, new Path((outputFile + "/" + task2Name)))
  job2.waitForCompletion(true)
  val job2a = Job.getInstance(configuration1,"word count")
  job2a.setJarByClass(classOf[Task2])
  job2a.setMapperClass(classOf[Task2Mapper2])
  job2a.setReducerClass(classOf[Task2Reducer2])
  job2a.setMapOutputKeyClass(classOf[IntWritable])
  job2a.setMapOutputValueClass(classOf[Text])
  job2a.setOutputKeyClass(classOf[Text])
  job2a.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(job2a, new Path(outputFile + "/" + task2Name))
  FileOutputFormat.setOutputPath(job2a, new Path(outputFile + "/" + task2NameMR2))
  job2a.waitForCompletion(true)

  /**Task 3: To find the count of generated log messages for each message type.
  **/

  val job3: Job = Job.getInstance(configure, task3Name)
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
  FileOutputFormat.setOutputPath(job3, new Path((outputFile + "/" + task3Name)))

  /**
  Task 4: To produce the number of characters in each log message for each log message type that contain the
  highest number of characters in the detected instances of the designated regex pattern.
   **/

  val job4: Job = Job.getInstance(configure, task4Name)
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
  FileOutputFormat.setOutputPath(job4, new Path((outputFile + "/" + task4Name)))

  if (job1.waitForCompletion(true) && job2.waitForCompletion(true)  && job3.waitForCompletion(true) && job4.waitForCompletion(true)) {
    logger.info("ALL JOBS SUCCESSFULLY COMPLETED")
  } else {
    logger.info("UNFORTUNATELY FAILED")
  }
}
}