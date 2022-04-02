package station

import config.RailTransitTable
import jdbc.{MysqlConf, OracleConf}
import org.apache.spark.sql.Dataset

class StationLoader(oracleConf: OracleConf = null, mysqlConf: MysqlConf = null) {
  def this(oracleConf: OracleConf) {
    this(oracleConf, null)
  }

  def this(mysqlConf: MysqlConf) {
    this(null, mysqlConf)
  }

  def load(stationTable: String): Dataset[StationInfo] = {
    if (oracleConf == null) {
      throw new RuntimeException("if you want to reload stationTable,please init the oracleConf!")
    }
    val stationFrame = oracleConf.load(stationTable)
    import stationFrame.sparkSession.implicits._
    stationFrame.map(x => {
      val id = x.getAs[java.math.BigDecimal]("CZ_ID").intValue()
      val name = x.getAs[String]("CZ_NAME")
      val line_name = x.getAs[String]("LJM")
      val `type` = x.getAs[String]("CZ_XZ")
      StationInfo(id, name, line_name, `type`)
    })
  }

  def reload(stationTable: String): Dataset[StationInfo] = {
    if (mysqlConf == null) {
      throw new RuntimeException("if you want to reload stationTable,please init the mysqlConf!")
    }
    val stationFrame = mysqlConf.load(stationTable)
    import stationFrame.sparkSession.implicits._
    stationFrame.as
  }
}

object StationLoader {
  val defaultStation: String = RailTransitTable.stationTable
}
