package section

import jdbc.{JDBCUtils, MysqlConf, OracleConf}
import org.apache.spark.sql.{Dataset, SparkSession}

class SectionWriter {
  def insert(sectionInfo: Dataset[SectionInfo], targetSection: String): Unit = {
    val sql =
      s"""
INSERT INTO $targetSection(id,station_in,station_out,section_direction,line_name,length) VALUES(?,?,?,?,?,?)
    """.stripMargin
    sectionInfo.foreachPartition((it: Iterator[SectionInfo]) => {
      val connection = JDBCUtils.getConnection
      connection.setAutoCommit(false)
      val preparedStatement = connection.prepareStatement(sql)
      it.foreach(x => {
        preparedStatement.setInt(1, x.id)
        preparedStatement.setInt(2, x.station_in)
        preparedStatement.setInt(3, x.station_out)
        preparedStatement.setInt(4, x.section_direction)
        preparedStatement.setString(5, x.line_name)
        preparedStatement.setDouble(6, x.length)
        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      connection.commit()
      JDBCUtils.closeConnection(connection, preparedStatement)
    })
  }

  def update(travelData: Dataset[TravelTimeInfo], sectionTable: String): Unit = {
    val sql =
      s"""
UPDATE $sectionTable SET travel_time=? WHERE id=?
    """.stripMargin
    travelData.foreachPartition((it: Iterator[TravelTimeInfo]) => {
      val connection = JDBCUtils.getConnection
      connection.setAutoCommit(false)
      val preparedStatement = connection.prepareStatement(sql)
      it.foreach(x => {
        val id = x.sectionId
        val travelTime = x.travelTime
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

object SectionWriter {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("WriteSection").getOrCreate()
    writeSectionByJdbc(sparkSession)
  }

  private def writeSectionByJdbc(sparkSession: SparkSession): Unit = {
    val oracleConf = new OracleConf(sparkSession)
    val mysqlConf = new MysqlConf(sparkSession)
    val loadSection = new SectionLoader(oracleConf, mysqlConf)
    val loadTable = "SCOTT.\"3-yxtSection\""
    val writeTable = "section_info"
  }
}
