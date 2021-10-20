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
  val configure1: Configuration = new Configuration()


  //Format as CSV output
  configure.set("mapred.textoutputformat.separator", ",")
  configure1.set("mapred.textoutputformat.separator", ",")

  val task1Name = conf.getString("configuration.task1")
  val task2Name = conf.getString("configuration.task2")
  val task2NameMR2 = conf.getString("configuration.task2MR2")
  val task3Name = conf.getString("configuration.task3")
  val task4Name = conf.getString("configuration.task4")

   /**Task 1: To find the count of generated log messages with injected regex pattern for each
   message type in a predefined time interval.
   **/

  val task1: Job = Job.getInstance(configure, task1Name)
  task1.setJarByClass(classOf[Task1])
  task1.setMapperClass(classOf[Task1Mapper])
  task1.setCombinerClass(classOf[Task1Reducer])
  task1.setPartitionerClass(classOf[Task1Partitioner])
  task1.setNumReduceTasks(2)
  task1.setReducerClass(classOf[Task1Reducer])
  task1.setOutputKeyClass(classOf[Text]);
  task1.setOutputValueClass(classOf[IntWritable]);
  task1.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
  FileInputFormat.addInputPath(task1, new Path(inputFile))
  FileOutputFormat.setOutputPath(task1, new Path((outputFile + "/" + task1Name)))

  /**Task 2: To display the time intervals sorted in the descending order
  that contained most log messages of the type ERROR with injected regex pattern string instances.
  **/

  val task2: Job = Job.getInstance(configure, task2Name)
  task2.setJarByClass(classOf[Task2])
  task2.setMapperClass(classOf[Task2Mapper1])
  task2.setCombinerClass(classOf[Task2Reducer1])
  task2.setPartitionerClass(classOf[Task2Partitioner])
  task2.setNumReduceTasks(2)
  task2.setReducerClass(classOf[Task2Reducer1])
  task2.setOutputKeyClass(classOf[Text])
  task2.setOutputValueClass(classOf[IntWritable])
  FileInputFormat.addInputPath(task2, new Path(inputFile))
  FileOutputFormat.setOutputPath(task2, new Path((outputFile + "/" + task2Name)))
  task2.waitForCompletion(true)
  val task2a = Job.getInstance(configure1,"word count")
  task2a.setJarByClass(classOf[Task2])
  task2a.setMapperClass(classOf[Task2Mapper2])
  task2a.setReducerClass(classOf[Task2Reducer2])
  task2a.setMapOutputKeyClass(classOf[IntWritable])
  task2a.setMapOutputValueClass(classOf[Text])
  task2a.setOutputKeyClass(classOf[Text])
  task2a.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(task2a, new Path(outputFile + "/" + task2Name))
  FileOutputFormat.setOutputPath(task2a, new Path(outputFile + "/" + task2NameMR2))
  task2a.waitForCompletion(true)

  /**Task 3: To find the count of generated log messages for each message type.
  **/

  val task3: Job = Job.getInstance(configure, task3Name)
  task3.setJarByClass(classOf[Task3])
  task3.setMapperClass(classOf[Task3Mapper])
  task3.setCombinerClass(classOf[Task3Reducer])
  task3.setPartitionerClass(classOf[Task3Partitioner])
  task3.setNumReduceTasks(2)
  task3.setReducerClass(classOf[Task3Reducer])
  task3.setOutputKeyClass(classOf[Text])
  task3.setOutputValueClass(classOf[IntWritable])
  task3.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
  FileInputFormat.addInputPath(task3, new Path(inputFile))
  FileOutputFormat.setOutputPath(task3, new Path((outputFile + "/" + task3Name)))

  /**
  Task 4: To produce the number of characters in each log message for each log message type that contain the
  highest number of characters in the detected instances of the designated regex pattern.
   **/

  val task4: Job = Job.getInstance(configure, task4Name)
  task4.setJarByClass(classOf[Task4])
  task4.setMapperClass(classOf[Task4Mapper])
  task4.setCombinerClass(classOf[Task4Reducer])
  task4.setPartitionerClass(classOf[Task4Partitioner])
  task4.setNumReduceTasks(2)
  task4.setReducerClass(classOf[Task4Reducer])
  task4.setOutputKeyClass(classOf[Text])
  task4.setOutputValueClass(classOf[IntWritable])
  task4.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
  FileInputFormat.addInputPath(task4, new Path(inputFile))
  FileOutputFormat.setOutputPath(task4, new Path((outputFile + "/" + task4Name)))

  //Once all the tasks are run the success message is logged
  if (task1.waitForCompletion(true) && task2.waitForCompletion(true)  && task3.waitForCompletion(true) && task4.waitForCompletion(true)) {
    logger.info("ALL JOBS SUCCESSFULLY COMPLETED")
  } else {
    logger.info("UNFORTUNATELY FAILED")
  }
}
}