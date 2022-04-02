package section

import jdbc.{JDBCUtils, MysqlConf, OracleConf}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class TravelTime(oracleConf: OracleConf, mysqlConf: MysqlConf) {
  /**
    * 加载运行时间
    *
    * @param travelTable  运行图表
    * @param sectionTable 区间表
    * @return
    */
  def load(travelTable: String, sectionTable: String): DataFrame = {
    val timetable = oracleConf.load(travelTable)
    val sectionFrame = mysqlConf.load(sectionTable)
    val sparkSession = sectionFrame.sparkSession
    import sparkSession.implicits._
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
    val sectionTravelTimeFrame = sparkSession.sql(sectionTravelTimeSql)
    val sectionTravelDataSet = sectionTravelTimeFrame
      .filter(sectionTravelTime => {
        val departureTime = sectionTravelTime.getAs[String]("DEP_TIME")
        val arrivalTime = sectionTravelTime.getAs[String]("ARR_TIME")
        if (departureTime.contains("-") || arrivalTime.contains("-")) {
          false
        } else {
          true
        }
      })
      .map(sectionTravelLine => {
        val inId = sectionTravelLine.getAs[java.math.BigDecimal]("CZ1_ID").intValue().toString
        val outId = sectionTravelLine.getAs[java.math.BigDecimal]("CZ2_ID").intValue().toString
        val departureTime = sectionTravelLine.getAs[String]("DEP_TIME")
        val arrivalTime = sectionTravelLine.getAs[String]("ARR_TIME")
        val seconds = TimeConverter.convertSecondsTime(departureTime, arrivalTime)
        val section = Section(inId, outId)
        val sectionTravelTime = TimeConverter(section, seconds)
        sectionTravelTime
      })
    val travelFrame = sectionTravelDataSet.select("section.inId", "section.outId", "seconds")
      .groupBy("inId", "outId")
      .agg(expr("percentile(seconds, array(0.5))[0]").alias("percentile_seconds"))
    travelFrame.createOrReplaceTempView("travel_time")
    sectionFrame.createOrReplaceTempView("section_info")
    val sectionInfo = sparkSession.sql(
      """
SELECT id,CAST(percentile_seconds AS INT) travel_time
FROM section_info JOIN travel_time ON station_in=inId AND station_out=outId
      """.stripMargin)
    sectionInfo
  }
}

object TravelTime {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("WriteSection").getOrCreate()
    val sectionTable: String = "section_info"
    val dataFrame = loadTravelGraph(sparkSession, sectionTable)
    updateSection(dataFrame, sectionTable)
  }

  private def loadTravelGraph(sparkSession: SparkSession, sectionTable: String) = {
    val oracleConf = new OracleConf(sparkSession)
    val mysqlConf = new MysqlConf(sparkSession)
    val loadTravelTime = new TravelTime(oracleConf, mysqlConf)
    val travelTable = "SCOTT.\"7-yxtKhsk\""
    loadTravelTime.load(travelTable, sectionTable)
  }

  def updateSection(travelFrame: DataFrame, sectionTable: String): Unit = {
    val sql =
      s"""
UPDATE $sectionTable SET travel_time=? WHERE id=?
    """.stripMargin
    travelFrame.foreachPartition((it: Iterator[Row]) => {
      val connection = JDBCUtils.getConnection
      connection.setAutoCommit(false)
      val preparedStatement = connection.prepareStatement(sql)
      it.foreach(x => {
        val id = x.getAs[Int]("id")
        val travelTime = x.getAs[Int]("travel_time")
        preparedStatement.setInt(1, travelTime)
        preparedStatement.setInt(2, id)
        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      connection.commit()
      JDBCUtils.closeConnection(connection, preparedStatement)
    })
  }
}
