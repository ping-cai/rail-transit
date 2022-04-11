import jdbc.MysqlConf
import org.apache.spark.sql.SparkSession
import section.SectionLoader
import timetable.TimeTableLoader
import train.{TrainLoader, TrainOperationWriter}

object TrainOperationWriterTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("TrainOperationWriterTest").getOrCreate()
    val mysqlConf = new MysqlConf(sparkSession)
    val sectionLoader = new SectionLoader(mysqlConf)
    val timeTableLoader = new TimeTableLoader(mysqlConf)
    val trainLoader = new TrainLoader(mysqlConf)
    val trainOperationWriter = new TrainOperationWriter(sectionLoader, timeTableLoader, trainLoader)
    trainOperationWriter.write()
  }
}
