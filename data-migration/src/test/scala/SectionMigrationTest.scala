import jdbc.{MysqlConf, OracleConf}
import org.apache.spark.sql.SparkSession
import section.{SectionLoader, SectionMigration, SectionWriter}

object SectionMigrationTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("SectionMigrationTest").getOrCreate()
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
