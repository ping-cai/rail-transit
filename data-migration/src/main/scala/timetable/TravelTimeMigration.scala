package timetable

import jdbc.{JDBCUtils, OracleConf}
import org.apache.spark.sql.{Dataset, Row}

class TravelTimeMigration(oracleConf: OracleConf) {
  def loadTravel(travelTable: String): Dataset[TravelTimeInfo] = {
    val timetable = oracleConf.load(travelTable)
    val sparkSession = timetable.sparkSession
    import timetable.sparkSession.implicits._
    timetable.createOrReplaceTempView("yxtKhsk")
    val sectionTravelTimeSql =
      """
select origin.CZ_ID CZ1_ID,origin.DEP_TIME,target.CZ_ID CZ2_ID,target.ARR_TIME
FROM yxtKhsk origin
join yxtKhsk target on origin.LCXH=target.LCXH AND origin.XH=target.XH -1
GROUP BY origin.CZ_ID,origin.CZ_NAME,origin.DEP_TIME,
target.CZ_ID,target.CZ_NAME,target.ARR_TIME,origin.LCXH,origin.XH
ORDER BY origin.LCXH,origin.XH
    """.stripMargin
    sparkSession.sql(sectionTravelTimeSql)
      .filter(sectionTravelTime => {
        val departureTime = sectionTravelTime.getAs[String]("DEP_TIME")
        val arrivalTime = sectionTravelTime.getAs[String]("ARR_TIME")
        if (departureTime.contains("-") || arrivalTime.contains("-")) {
          false
        } else {
          true
        }
      })
      .mapPartitions((partition: Iterator[Row]) => {
        partition.map(
          sectionTravelLine => {
            val inId = sectionTravelLine.getAs[java.math.BigDecimal]("CZ1_ID").intValue().toString
            val outId = sectionTravelLine.getAs[java.math.BigDecimal]("CZ2_ID").intValue().toString
            val departureTime = sectionTravelLine.getAs[String]("DEP_TIME")
            val arrivalTime = sectionTravelLine.getAs[String]("ARR_TIME")
            TravelTimeInfo(inId, outId, departureTime, arrivalTime)
          }
        )
      })
  }

  def migrate(): Unit = {
    val travelData: Dataset[TravelTimeInfo] = loadTravel(TravelTimeMigration.defaultMigrationOriginTable)
    travelData.foreachPartition((partition: Iterator[TravelTimeInfo]) => {
      val connection = JDBCUtils.getConnection
      connection.setAutoCommit(false)
      val preparedStatement = connection.prepareStatement(TravelTimeMigration.migrateSql)
      partition.foreach(timeTable => {
        preparedStatement.setString(1, timeTable.inId)
        preparedStatement.setString(2, timeTable.outId)
        preparedStatement.setString(3, timeTable.departureTime)
        preparedStatement.setString(4, timeTable.arrivalTime)
        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      connection.commit()
      JDBCUtils.closeConnection(connection, preparedStatement)
    })
  }
}

object TravelTimeMigration {
  val defaultMigrationOriginTable = "SCOTT.\"7-yxtKhsk\""
  val defaultMigrationTargetTable = "time_table"
  val migrateSql: String =
    s"""
       |INSERT INTO $defaultMigrationTargetTable(start_station_id,end_station_id,departure_time,arrival_time)
       |VALUES (?,?,?,?)
    """.stripMargin
}