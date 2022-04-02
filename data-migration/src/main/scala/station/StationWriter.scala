package station

import jdbc.JDBCUtils
import org.apache.spark.sql.Dataset

class StationWriter extends Serializable {
  val sql: String =
    s"""
INSERT INTO station_info(id,name,line_name,type) VALUES(?,?,?,?)
    """.stripMargin

  def insert(stationInfo: Dataset[StationInfo]): Unit = {
    insert(stationInfo, sql)
  }

  def insert(stationInfo: Dataset[StationInfo], sql: String): Unit = {
    stationInfo.foreachPartition((it: Iterator[StationInfo]) => {
      val connection = JDBCUtils.getConnection
      connection.setAutoCommit(false)
      val preparedStatement = connection.prepareStatement(sql)
      it.foreach(x => {
        preparedStatement.setInt(1, x.id)
        preparedStatement.setString(2, x.name)
        preparedStatement.setString(3, x.line_name)
        preparedStatement.setString(4, x.`type`)
        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      connection.commit()
      JDBCUtils.closeConnection(connection, preparedStatement)
    })
  }
}
