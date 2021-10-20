## CS441 - Engineering Distributed Objects for Cloud Computing

## HomeWork 2 - Map Reduce

###Introduction
This homework's purpose is to perform map/reduce operations in order to generate various statistics for Log File.
Task 1, Task 2, Task 3, and Task 4 are the numerous tasks here. The log file is located in the src/main/resources directory.

EMR Deployment Demo :

###Environment
OS: Windows 10

IDE: IntelliJ IDEA Ultimate 2018.3

Hypervisor: VMware Workstation 16 Pro

Hadoop Distribution: Hortonworks Data Platform (3.0.1) Sandbox deployed on VMware

Log files (present under src/main/resources of this project)

###Running the project

1) Clone this repository 

```
git clone https://github.com/RamiyaShreeSeshaiah/MapReduce_CS441.git
```
```
cd MapReduce_CS441
```

2) Open the project in intelliJ

3) Generate jar file by running the following command in the terminal

````
sbt clean compile assembly
````

###Running the jobs
1) Start HDP Sandbox VM

2) Once HDP Sandbox VM is started, to initiate Hortonworks Sandbox session, open the address mentioned in VMware workstation and enter the login credentials

3) Once the session is started, Make a directory in the root 
````
mkdir MapReduce
````

2) Move to this directory
````
cd MapReduce
````
3) Copy jar file generated to this directory in the terminal/command prompt

scp -P 2222 <Path/LocalJarFile> root@192.168.133.128:<Destination/Path>

``````
scp -P 2222 .target/scala-2.13/MyMapReduce.jar root@192.168.133.128:/root/MapReduce
``````

4) Move the Input Log File to this directory

scp -P 2222 <Path/InputFile> root@192.168.133.128:<Destination/Path>

````
scp -P 2222 .\LogFileGenerator.2021-10-*.log root@192.168.133.128:/root/MapReduce
````
5) Create HDFS Directory in VM:

     hadoop fs -mkdir </InputDirectory/>

<InputDirectory> is mentioned in the application.conf( inputFile = "inputDir"). You can change the input directory name in the application.conf or use the same as given. 

````````
hadoop fs -mkdir inputDir 
````````

6) Now,Copy the input file to Hadoop HDFS File System.

   hadoop fs -put </LogFilePath/> </InputDirectory/>
   
   LogFilePath : The InputFile path in the MapReduce directory created

```
hadoop fs -put LogFileGenerator.2021-10-*.log inputDir
````
7) Run Map-Reduce Tasks

   hadoop jar </nameOfJarFile/>

   nameOfJarFile : Is the jar file in the MapReduce directory created

````
hadoop jar MyMapReduce.jar
````

8) To view the output use the following commands

   hdfs dfs -get </OutputDirectory/> </outputPath/>

   OutputDirectory : This is mentioned in the application.conf(outputFile = "outputDir"). You can change the output directory name in the application.conf or use the same as given. 
   
   OutputPath : The path to copy the output

``
hdfs dfs -get outputDir mapReduceOutput 
``

Now output is copied to mapReduceOutput from outputDir

9) Generating .csv output file

      9.1) Move to output path in which the output is copied and ls to see the output of each task
 
              cd mapReduceOutput
      
     9.2) To generate .csv output file
 
     ````
       hadoop fs -cat outputDir/task1/* > Task1.csv  
    ````
(Same way for all the tasks)

Now, mapReduceOutput will have all the output files in both normal and .csv format

10) Copy the output file to localMachine

In the terminal, move to the path in which you want to copy the file and paste the following command

````
scp -P 2222 -r root@192.168.133.128:/root/MapReduce/mapReduceOutput /mapReduceOutputFiles
````

### File Structure

1) Input files(LogFiles) : /src/main/resources/LogFileGenerator.2021-10-*.log
2) Application Configuration : /src/main/resources/application.conf
3) Tasks - /src/main/scala/com.ramiya

   3.1) Task1 - /src/main/scala/com.ramiya/Task1

   3.2) Task2 - /src/main/scala/com.ramiya/Task2
 
   3.3) Task3 - /src/main/scala/com.ramiya/Task3
 
   3.4) Task4 - /src/main/scala/com.ramiya/Task4
 
   3.5) MapperReducerTasks - /src/main/scala/MapperReducerTasks : This is the main map reduce class that runs every task
5) MapReduceTest - /src/test/scala/MapReduceTest : Map reduce tests
6) Jar File - /target/scala-2.13/MyMapReduce.jar : Generated Jar files


### Map Reduce Tasks Implementation

There are totally 4 tasks created in this homework. They are clearly listed below

Firstly, the regex pattern is checked across the log entry for all four tasks. If it succeeds, it moves on to the job computation.
Note : regex pattern is mentioned in the application.conf

Mapper: The input data is first processed by all Mappers/Map tasks, and then the intermediate output is generated.

Combiner : Before the shuffle/sort phase, the Combiner optimizes all intermediate outputs using local aggregation. Combiners' main purpose is to reduce bandwidth by reducing the number of key/value pairs that must be shuffled across the network and delivered as input to the Reducer.

Partitioner: Partitioner controls the partitioning of the keys of the intermediate map output in Hadoop. To determine partition, the hash function is employed. Each map output is partitioned based on the key-value pair. Each partition (inside each mapper) contains records with the same key value, and each partition is subsequently forwarded to a Reducer. Partition phase takes place in between mapper and reducer.
The Hash Partitioner (Default Partitioner) computes a hash value for the key and assigns the partition based on it.

I have used two custom partitioner logic
MyCustomPartitioner logic1(For Task1, Task3, Task4): Based on the logs generated INFO relatively had higher log messages than ERROR/WARN/DEBUG so the log messages of INFO is sent to one reduce task and the other log messages are sent to the other reduce task
MyCustomPartitioner logic2(For Task2): Here, partitioning is done based on the hours, 1-12 hour is sent to one reduce task and 13-24 for another reduce task

Reducer :
In Hadoop MapReduce, a reducer condenses a set of intermediate values that share a key into a smaller set. Reducer takes a set of intermediate key-value pairs produced by the mapper as input in the MapReduce job execution flow. Reducer then aggregates, filters, and combines key-value pairs, which necessitates extensive processing.

1) Task 1: To find the count of generated log messages with injected regex pattern for each message type in a predefined time interval.

A predefined time interval is mentioned in the application.conf. Using this start and end time, the log messages with that injected regex pattern for every log message tag is counted.

Task1Mapper: (Key, Value) (key -> logMessageTag - [ERRO/INFO/WARN/DEBUG]), (value -> 1)

Task1Reducer : (Key, Value) (key -> logMessageTag - [ERRO/INFO/WARN/DEBUG]) , (value -> sum of the logMessage count)

Task1Partitioner : (Key, Value) (Key ->  logMessageTag - [ERRO/INFO/WARN/DEBUG]) , (value -> sum of the logMessage count, NumReduceTasks : 2)

2) Task 2: To display the time intervals sorted in the descending order that contained most log messages of the type ERROR with injected regex pattern string instances. 

Every one hour is chosen as the time interval here. So, for every hour the ERROR tag injected regex pattern log messages is counted and displayed in descending order based on the count.

Task2Mapper1: (Key, Value) (key -> Hour - [1..24]), (value -> 1)

Task2Reducer1 : (Key, Value) (key -> Hour - [1..24]) , (value -> sum of the Error tag logMessage count)

Task2Partitioner : (Key, Value) (Key ->  Hour - [1..24]) , (value -> sum of the logMessage count, NumReduceTasks : 2)

Task2Mapper2: (Key, Value) (key -> sum of the Error tag logMessage count * -1 ), (value -> hour [1..24])

In Task2Reducer2, the sorted is done based on the key value sent by the Task2Mapper2

Task2Reducer2 : (Key, Value) (key -> Hour - [1..24]) , (value -> sum of the Error tag logMessage count)

3) Task 3: To find the count of generated log messages for each message type.

The log messages for every log message tag is counted

Task3Mapper: (Key, Value) (key -> logMessageTag - [ERRO/INFO/WARN/DEBUG]), (value -> 1)

Task3Reducer : (Key, Value) (key -> logMessageTag - [ERRO/INFO/WARN/DEBUG]) , (value -> sum of the logMessage count)

Task3Partitioner : (Key, Value) (Key -> logMessageTag - [ERRO/INFO/WARN/DEBUG]) , (value -> sum of the logMessage count, NumReduceTasks : 2)

4) Task 4: To produce the number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern.

The max length of every injected regex pattern log message for every log message is displayed

Task4Mapper: (Key, Value) (key -> logMessageTag - [ERRO/INFO/WARN/DEBUG]), (value -> logMessageLength)

Task4Reducer : (Key, Value) (key -> logMessageTag - [ERRO/INFO/WARN/DEBUG]) , (value -> max of the logMessageLength)

Task4Partitioner : (Key, Value) (Key -> logMessageTag - [ERRO/INFO/WARN/DEBUG]) , (value -> max of the logMessageLength, NumReduceTasks : 2)


### Output

The output of every task can be located under OutputFiles folder
You can see two output files from reach reduce task.

Task1/Task3/Task4 : One output file containing ERROR/DEBUG/WARN and another with INFO.
Task2 : One output file would be empty because my logfile doesn't have any hour between (1-12) but the other file with 13,15,17,18 which is between(13-24).

