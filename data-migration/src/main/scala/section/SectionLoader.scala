package section

import jdbc.{MysqlConf, OracleConf}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.expr

class SectionLoader {
  def this(oracleConf: OracleConf) {
    this()
    this.oracleConf = oracleConf
  }

  def this(mysqlConf: MysqlConf) {
    this()
    this.mysqlConf = mysqlConf
  }

  def this(oracleConf: OracleConf, mysqlConf: MysqlConf) {
    this(oracleConf)
    this.mysqlConf = mysqlConf
  }

  var oracleConf: OracleConf = _
  var mysqlConf: MysqlConf = _

  def defaultLoad(): Dataset[SectionInfo] = {
    val sparkSession = mysqlConf.sparkSession
    import sparkSession.implicits._
    mysqlConf.load(SectionLoader.sectionTable).as
  }

  def load(sectionTable: String): Dataset[SectionInfo] = {
    val sectionFrame = oracleConf.load(sectionTable)
    val sparkSession = sectionFrame.sparkSession
    import sparkSession.implicits._
    val sectionInfo = sectionFrame.map(x => {
      val sectionId = x.getAs[java.math.BigDecimal]("QJ_ID").intValue()
      val stationIn = x.getAs[java.math.BigDecimal]("CZ1_ID").intValue()
      val stationOut = x.getAs[java.math.BigDecimal]("CZ2_ID").intValue()
      val sectionDirection = x.getAs[java.math.BigDecimal]("QJ_SXX").intValue()
      val lineName = x.getAs[String]("QJ_LJM")
      val length = x.getAs[java.math.BigDecimal]("QJ_LENGTH").doubleValue()
      SectionInfo(sectionId, stationIn, stationOut,
        sectionDirection, lineName, length)
    })
    sectionInfo
  }

  /**
    * 加载运行时间
    *
    * @param travelTable  运行图表
    * @param sectionTable 区间表
    * @return
    */
  def load(travelTable: String, sectionTable: String): Dataset[TravelTimeInfo] = {
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
    sectionInfo.map(x => {
      val sectionId = x.getAs[Int]("id")
      val travelTime = x.getAs[Int]("travel_time")
      TravelTimeInfo(sectionId, travelTime)
    })
  }


}

object SectionLoader {
  val sectionTable = "section_info"
}
