package afc

import org.apache.spark.sql.Dataset
import station.StationInfo

object Transform {
  def transform(afcStationInfo: Dataset[StationInfo], stationInfo: Dataset[StationInfo]): Dataset[TransformInfo] = {
    val afc_station = "afc_station"
    afcStationInfo.createOrReplaceTempView(afc_station)
    val station_info = "station_info"
    stationInfo.createOrReplaceTempView(station_info)
    val sparkSession = afcStationInfo.sparkSession
    import sparkSession.implicits._
    sparkSession
      .sql(
        s"""
           |SELECT station_info.id station_id,afc_station.id afc_id
           |FROM $afc_station JOIN $station_info ON
           |afc_station.name = station_info.name AND afc_station.line_name = station_info.line_name
      """.stripMargin)
      .map(x => {
        val station_id = x.getAs[Int]("station_id")
        val afc_id = x.getAs[Int]("afc_id")
        TransformInfo(station_id, afc_id)
      })
  }
}
