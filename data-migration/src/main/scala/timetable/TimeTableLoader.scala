package timetable

import jdbc.MysqlConf
import org.apache.spark.sql.Dataset

class TimeTableLoader(mysqlConf: MysqlConf) {
  def load(): Dataset[TimeTableInfo] = {
    val dataFrame = mysqlConf.load(TimeTableLoader.timeTable)
    val sparkSession = dataFrame.sparkSession
    import sparkSession.implicits._
    dataFrame.as
  }
}

object TimeTableLoader {
  val timeTable = "time_table"
}
