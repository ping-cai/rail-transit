package flow

import java.sql.Date

import config.HdfsConf
import crowding.CrowdDegreeService
import jdbc.{JDBCUtils, MysqlConf}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import section.SectionLoader
import train.{TrainLoader, TrainOperationLoader}

class FlowMigration(flowLoader: FlowLoader, crowdDegreeService: CrowdDegreeService) {
  private val sectionLoader = crowdDegreeService.sectionLoader

  private def migrateSection(sectionFlow: Dataset[SectionFlow], granularity: Int): Unit = {
    val sparkSession = sectionFlow.sparkSession
    val sectionInfo = sectionLoader.defaultLoad()
    sectionFlow.where(s"granularity=$granularity")
      .createOrReplaceTempView("section_flow")
    sectionInfo.createOrReplaceTempView("section_info")
    val sectionSaveFrame = sparkSession.sql(
      """
SELECT section_info.id section_id,section_flow.start_time,section_flow.end_time,section_flow.flow,
section_flow.ds
FROM section_flow join section_info ON section_info.station_in = section_flow.start_station_id
AND section_info.station_out = section_flow.end_station_id
      """.stripMargin)
    sectionSaveFrame.foreachPartition((partition: Iterator[Row]) => {
      val sql = FlowMigration.insertSection(granularity)
      val connection = JDBCUtils.getConnection
      connection.setAutoCommit(false)
      val preparedStatement = connection.prepareStatement(sql)
      partition.foreach(x => {
        preparedStatement.setInt(1, x.getAs[Int]("section_id"))
        preparedStatement.setTimestamp(2, x.getAs[java.sql.Timestamp]("start_time"))
        preparedStatement.setTimestamp(3, x.getAs[java.sql.Timestamp]("end_time"))
        preparedStatement.setDouble(4, x.getAs[Double]("flow"))
        preparedStatement.setDate(5, x.getAs[Date]("ds"))
        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      connection.commit()
      JDBCUtils.closeConnection(connection, preparedStatement)
    })
  }

  private def migrateStation(stationFlow: Dataset[StationFlow], granularity: Int): Unit = {
    stationFlow.where(s"granularity=$granularity")
      .foreachPartition((partition: Iterator[StationFlow]) => {
        val sql = FlowMigration.insertStation(granularity)
        val connection = JDBCUtils.getConnection
        connection.setAutoCommit(false)
        val preparedStatement = connection.prepareStatement(sql)
        partition.foreach(x => {
          preparedStatement.setInt(1, x.station_id.toInt)
          preparedStatement.setTimestamp(2, x.start_time)
          preparedStatement.setTimestamp(3, x.end_time)
          preparedStatement.setDouble(4, x.in_flow)
          preparedStatement.setDouble(5, x.out_flow)
          preparedStatement.setDate(6, new Date(x.start_time.getTime))
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        connection.commit()
        JDBCUtils.closeConnection(connection, preparedStatement)
      })
  }

  private def migrateTransfer(transferFlow: Dataset[TransferFlow], granularity: Int): Unit = {
    transferFlow.where(s"granularity=$granularity")
      .foreachPartition((partition: Iterator[TransferFlow]) => {
        val sql = FlowMigration.insertTransfer(granularity)
        val connection = JDBCUtils.getConnection
        connection.setAutoCommit(false)
        val preparedStatement = connection.prepareStatement(sql)
        partition.foreach(x => {
          preparedStatement.setInt(1, x.transfer_out_station_id.toInt)
          preparedStatement.setInt(2, x.transfer_in_station_id.toInt)
          preparedStatement.setTimestamp(3, x.start_time)
          preparedStatement.setTimestamp(4, x.end_time)
          preparedStatement.setDouble(5, x.flow)
          preparedStatement.setDate(6, new Date(x.start_time.getTime))
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        connection.commit()
        JDBCUtils.closeConnection(connection, preparedStatement)
      })
  }

  def migrateFlow(): Unit = {
    val sectionFlow = flowLoader.loadAllSection()
    val stationFlow = flowLoader.loadAllStation()
    val transferFlow = flowLoader.loadAllTransfer()
    List(15, 30, 60).foreach(x => {
      crowdDegreeService.setCrowdDegree(sectionFlow, x)
      migrateStation(stationFlow, x)
      migrateTransfer(transferFlow, x)
    })
  }

  def migrateFlow(date: String): Unit = {
    val sectionFlow = FlowMigration.loadDate(flowLoader.loadAllSection(), date)
    val stationFlow = FlowMigration.loadDate(flowLoader.loadAllStation(), date)
    val transferFlow = FlowMigration.loadDate(flowLoader.loadAllTransfer(), date)
    List(15, 30, 60).foreach(x => {
      crowdDegreeService.setCrowdDegree(sectionFlow, x)
      migrateStation(stationFlow, x)
      migrateTransfer(transferFlow, x)
    })
  }
}

object FlowMigration {
  val sectionTablePrefix = "section_flow"
  val stationTablePrefix = "station_flow"
  val transferTablePrefix = "transfer_flow"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("FlowMigration").getOrCreate()
    val hdfsConf = new HdfsConf(sparkSession)
    val flowLoader = new FlowLoader(hdfsConf)
    val mysqlConf = new MysqlConf(sparkSession)
    val sectionLoader = new SectionLoader(mysqlConf)
    val trainLoader = new TrainLoader(mysqlConf)
    val trainOperationLoader = new TrainOperationLoader(mysqlConf)
    val crowdDegreeService = new CrowdDegreeService(sectionLoader, trainLoader, trainOperationLoader)
    val flowMigration = new FlowMigration(flowLoader, crowdDegreeService)
    if (args.isEmpty) {
      flowMigration.migrateFlow()
    } else {
      flowMigration.migrateFlow()
    }
  }

  def insertSection(granularity: Int): String = {
    s"""
       |INSERT INTO ${sectionTablePrefix}_$granularity(section_id,start_time,end_time,flow,ds,crowd_degree)
       |VALUES (?,?,?,?,?,?)
    """.stripMargin
  }

  def insertStation(granularity: Int): String = {
    s"""
       |INSERT INTO ${stationTablePrefix}_$granularity(station_id,start_time,end_time,in_flow,out_flow,ds)
       |VALUES (?,?,?,?,?,?)
    """.stripMargin
  }

  def insertTransfer(granularity: Int): String = {
    s"""
       |INSERT INTO ${transferTablePrefix}_$granularity(transfer_out_station_id,transfer_in_station_id,start_time,end_time,flow,ds)
       |VALUES (?,?,?,?,?,?)
    """.stripMargin
  }

  def loadDate[T](data: Dataset[T], date: String): Dataset[T] = {
    data.where(s"ds='$date'")
  }

}
