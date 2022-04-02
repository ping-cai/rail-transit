package afc

import jdbc.JDBCUtils
import org.apache.spark.sql.Dataset
import station.{StationInfo, StationLoader, StationWriter}

class AfcStationWriter(loader: StationLoader, writer: StationWriter) extends Serializable {
  val chongqing_station_info = "chongqing_station_info"
  val chongqing_station_info_sql: String =
    s"""
INSERT INTO $chongqing_station_info(id,name,line_name,type) VALUES(?,?,?,?)
    """.stripMargin
  val station_transform = "station_transform"
  val station_transform_sql: String =
    s"""
       |INSERT INTO $station_transform(station_id,afc_id) VALUES(?,?)
    """.stripMargin

  def insert(stationInfo: Dataset[StationInfo]): Unit = {
    writer.insert(stationInfo, chongqing_station_info_sql)
  }

  def insertTransform(afcStationInfo: Dataset[StationInfo], stationInfo: Dataset[StationInfo]): Unit = {
    val transformInfo = Transform.transform(afcStationInfo, stationInfo)
    transformInfo.foreachPartition((it: Iterator[TransformInfo]) => {
      val connection = JDBCUtils.getConnection
      connection.setAutoCommit(false)
      val preparedStatement = connection.prepareStatement(station_transform_sql)
      it.foreach(x => {
        preparedStatement.setInt(1, x.station_id)
        preparedStatement.setInt(2, x.afc_id)
        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      connection.commit()
      JDBCUtils.closeConnection(connection, preparedStatement)
    })
  }
}
