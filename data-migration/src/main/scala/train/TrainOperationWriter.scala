package train

import jdbc.JDBCUtils
import org.apache.spark.sql.{Dataset, Row}
import section.{SectionInfo, SectionLoader}
import timetable.{TimeTableInfo, TimeTableLoader}

import scala.util.Random

class TrainOperationWriter(sectionLoader: SectionLoader,
                           timeTableLoader: TimeTableLoader,
                           trainLoader: TrainLoader) {
  def write(): Unit = {
    val sectionInfo: Dataset[SectionInfo] = sectionLoader.defaultLoad()
    val timeTableInfo: Dataset[TimeTableInfo] = timeTableLoader.load()
    val sparkSession = sectionInfo.sparkSession
    sectionInfo.createOrReplaceTempView("section_info")
    timeTableInfo.createOrReplaceTempView("time_table")
    val trainOperationFrame = sparkSession.sql(
      """
SELECT id section_id,departure_time start_time,arrival_time end_time
FROM section_info JOIN time_table
ON station_in = start_station_id
AND station_out=end_station_id
      """.stripMargin)
    val trainInfo = trainLoader.load(TrainLoader.trainTable)
    val trainInfoes = trainInfo.collect()
    val random = new Random()
    trainOperationFrame.foreachPartition((partition: Iterator[Row]) => {
      val connection = JDBCUtils.getConnection
      connection.setAutoCommit(false)
      val preparedStatement = connection.prepareStatement(TrainOperationWriter.writeSql)
      partition.foreach(x => {
        val section_id = x.getAs[Int]("section_id")
        val start_time = x.getAs[String]("start_time")
        val end_time = x.getAs[String]("end_time")
        val trainId = trainInfoes(random.nextInt(trainInfoes.length)).id
        val departure_times = start_time.split("\\.")
        val arrival_times = end_time.split("\\.")
        val departure_time = TrainOperationWriter.correctTime(departure_times)
        val arrival_time = TrainOperationWriter.correctTime(arrival_times)
        preparedStatement.setInt(1, section_id)
        preparedStatement.setString(2, departure_time)
        preparedStatement.setString(3, arrival_time)
        preparedStatement.setInt(4, trainId)
        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      connection.commit()
      JDBCUtils.closeConnection(connection, preparedStatement)
    })
  }
}

object TrainOperationWriter {
  val writeSql: String =
    """
      |INSERT INTO train_operation(section_id,start_time,end_time,train_id)
      |VALUES (?,?,?,?)
    """.stripMargin

  def correctTime(times: Array[String]): String = {
    times(0) = if (times(0).toInt < 10) {
      s"0${times(0)}"
    } else {
      times(0)
    }
    times.mkString(":")
  }
}
