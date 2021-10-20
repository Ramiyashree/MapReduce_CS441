import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import org.apache.hadoop.mapreduce.Job
import org.slf4j.{Logger, LoggerFactory}
import com.ramiya.{Task1, Task2, Task3, Task4}
import com.ramiya.Task1.{Task1Mapper, Task1Reducer}
import com.ramiya.Task2.{Task2Mapper1, Task2Mapper2, Task2Reducer1, Task2Reducer2}
import com.ramiya.Task3.{Task3Mapper, Task3Reducer}
import com.ramiya.Task4.{Task4Mapper, Task4Reducer}
import org.apache.hadoop.conf.Configuration
import org.scalatest.matchers.should.Matchers._

import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex


class MapReduceTest extends AnyFlatSpec with Matchers {

  val config = ConfigFactory.load("application.conf")

  it should "Check start and end time" in {
    val formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
    val startTime = LocalTime.parse(config.getString("configuration.startTime"), formatter)
    val endTime = LocalTime.parse(config.getString("configuration.endTime"), formatter)
    val time_test_pass = LocalTime.parse("13:02:53.657", formatter)
    val time_test_fail = LocalTime.parse("20:11:06.281", formatter)
    assert((startTime.isBefore(time_test_pass) && endTime.isAfter(time_test_pass)) &&
      (!(startTime.isBefore(time_test_fail) && endTime.isAfter(time_test_fail))))
  }

  it should "checking the mapper class" in {
    val configure: Configuration = new Configuration()
    val task1 = Job.getInstance(configure, "test1 mapReduce")
    task1.setMapperClass(classOf[Task1Mapper])
    assert(task1.getMapperClass() == classOf[Task1Mapper])
  }

  it should "checking the reducer class" in {
    val configure1: Configuration = new Configuration()
    val task1 = Job.getInstance(configure1, "test1 mapReduce")
    task1.setReducerClass(classOf[Task1Reducer])
    assert(task1.getReducerClass() == classOf[Task1Reducer])
  }

  it should "checking combiner class" in {
    val configure2: Configuration = new Configuration()
    val task1 = Job.getInstance(configure2, "test1 combiner")
    task1.setCombinerClass(classOf[Task1Reducer])
    assert(task1.getCombinerClass() == classOf[Task1Reducer])
  }

  it should "check regex pattern" in {
    val string = "13:02:53.767 [scala-execution-context-global-124] INFO  HelperUtils.Parameters$ - CC]>~R#,^#0JWyESarZdETDcvk)Yk'I?"
    val keyValPattern: Regex = config.getString("configuration.regexPattern").r

    val isPattern = keyValPattern.findFirstMatchIn(string) match {
      case Some(_) => true
      case None => false
    }
    assert(isPattern)
  }

  it should "check max value" in {
    val list = List(100, 200, 300, 400)
    val max = list.foldLeft(0)(_ max _)
    assert(max == 400)
  }

  it should "check sum value" in {
    val list = List(100, 200, 300, 400)
    val sum = list.foldLeft(0)(_ + _)
    assert(sum == 1000)
  }

}