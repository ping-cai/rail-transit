package afc

import jdbc.OracleConf
import org.apache.spark.sql.Dataset
import station.StationInfo

class AfcStationLoader(oracleConf: OracleConf) {
  def load(afcStationTable: String): Dataset[StationInfo] = {
    val afcStationFrame = oracleConf.load(afcStationTable)
    val afc_station = "afc_station"
    afcStationFrame.createOrReplaceTempView(afc_station)
    val sparkSession = afcStationFrame.sparkSession
    import sparkSession.implicits._
    sparkSession
      .sql(
        s"""
SELECT STATIONID id,TYPE type,CZNAME name,LINENAME line_name
FROM $afc_station
      """.stripMargin)
      .map(x => {
        val id = x.getAs[String]("id").toInt
        val name = x.getAs[String]("name")
        val line_name = x.getAs[String]("line_name")
        val `type` = x.getAs[String]("type")
        StationInfo(id, name, line_name, `type`)
      })
  }
}
