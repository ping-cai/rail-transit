import jdbc.OracleConf
import org.apache.spark.sql.SparkSession
import station.{StationLoader, StationMigration, StationWriter}

object StationMigrationTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("StationMigrationTest").getOrCreate()
    val oracleConf = new OracleConf(sparkSession)
    val stationLoader = new StationLoader(oracleConf)
    val stationWriter = new StationWriter
    val stationMigration = new StationMigration(stationLoader, stationWriter)
    val originStation = "SCOTT.\"2-yxtStation\""
    val targetStation = "station_info"
    stationMigration.migration(originStation, targetStation)
  }
}
