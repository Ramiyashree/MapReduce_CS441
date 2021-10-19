# MapReduce_CS441

CS441 - Engineering Distributed Objects for Cloud Computing

## CS441 - Engineering Distributed Objects for Cloud Computing


## HomeWork 2 - Map Reduce

###Overview
The goal of this homework is to run map/reduce tasks to produce various statistics for Log File.
The various tasks here are Task1, Task2, Task3 and Task4. The Log file can be found under src/main/resources

EMR Deployment Demo :

###Environment
OS: Windows 10

IDE: IntelliJ IDEA Ultimate 2018.3

Hypervisor: VMware Workstation 16 Pro

Hadoop Distribution: Hortonworks Data Platform (3.0.1) Sandbox deployed on VMware

Log files (present under src/main/resources of this project)

###Running the map reduce tasks

1) Clone this repository - git clone https://github.com/RamiyaShreeSeshaiah/MapReduce_CS441.git

2) Open the project in intelliJ
3) Generate jar file by running the following command in the terminal

sbt clean compile assembly
5) Start HDP Sanbox VM
6) Once HDP Sandbox VM is started, to initiate Hortonworks Sandbox session, open the address mentioned in VMware workstation and enter the login credentials
7) Copy jar file to HDP Sandbox VM. 
   scp -P 2222 .target/scala-2.13/MyMapReduce.jar root@192.168.133.128:<Destination/path>
8) Move the Input Log File to the destination
scp -P 2222 <Path/InputFile> root@192.168.133.128:<Destination/path>
9) Create HDFS Directory in VM:
hadoop fs -mkdir <inputDirectoryName>
   <inputDirectoryName> has mentioned in the code
10) Now, we need to copy the input file to Hadoop HDFS File System.
hadoop fs -put <input_file> <inputDirectoryName>
11) Run Map-Reduce Tasks
hadoop jar <nameOfJarFile>
12) To view the output use the following commands
    
    hdfs dfs -get <output_directory_AsGivenInTheCode> <destination_directory>

    scp -P 2222 -r root@192.168.96.128:/root/<destination_directory> /<localFileOutputFileName>

### Map Reduce Tasks

There are totally 4 tasks created in this homework. They are clearly listed below

1) Task 1: To find the count of generated log messages with injected regex pattern for each message type in a predefined time interval.

2) Task 2: To display the time intervals sorted in the descending order that contained most log messages of the type ERROR with injected regex pattern string instances. 

3) Task 3: To find the count of generated log messages for each message type.

4) Task 4: To produce the number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern.

### Output

Please find the output files located in this project under the folder named output_dir. Each job will have its own separate folder and the associated data output file.

hdfs dfs -get output_dir <destinationFile>

scp -P 2222 -r root@192.168.96.128:/root/<destinationFile> /<localSystemFile>

### Log File Format
The LogFiles generated are in the following format

TIME [scala-execution-context-global-124] LogMessageType  HelperUtils.Parameters$ - LogMessage
