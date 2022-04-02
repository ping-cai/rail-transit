package section

import jdbc.{MysqlConf, OracleConf}
import org.apache.spark.sql.SparkSession

class SectionMigration(loader: SectionLoader, writer: SectionWriter) {
  def migration(originSection: String, travelTable: String, targetSection: String): Unit = {
    val sectionInfo = loader.load(originSection)
    val travelTimeData = loader.load(travelTable, targetSection)
    writer.insert(sectionInfo, targetSection)
    writer.update(travelTimeData, targetSection)
  }
}

object SectionMigration {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("section.SectionMigration").getOrCreate()
    val oracleConf = new OracleConf(sparkSession)
    val mysqlConf = new MysqlConf(sparkSession)
    val sectionLoader = new SectionLoader(oracleConf, mysqlConf)
    val sectionWriter = new SectionWriter
    val originSection = "SCOTT.\"3-yxtSection\""
    val travelTable = "SCOTT.\"7-yxtKhsk\""
    val targetSection = "section_info"
    val sectionMigration = new SectionMigration(sectionLoader, sectionWriter)
    sectionMigration.migration(originSection, travelTable, targetSection)
  }
}
