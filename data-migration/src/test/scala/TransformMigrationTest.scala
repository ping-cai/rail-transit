import afc.{AfcStationLoader, AfcStationWriter, TransformMigration}
import jdbc.{MysqlConf, OracleConf}
import org.apache.spark.sql.SparkSession
import station.{StationLoader, StationWriter}

object TransformMigrationTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("TransformMigrationTest").getOrCreate()
    val oracleConf = new OracleConf(sparkSession)
    val mysqlConf = new MysqlConf(sparkSession)
    val afcStationLoader = new AfcStationLoader(oracleConf)
    val stationLoader = new StationLoader(oracleConf, mysqlConf)
    val stationWriter = new StationWriter
    val afcStationWriter = new AfcStationWriter(stationLoader, stationWriter)
    val transformMigration = new TransformMigration(afcStationLoader, afcStationWriter, stationLoader)
    val originAfcStation: String = "SCOTT.\"chongqing_stations_nm\""
    val targetAfcStation: String = "chongqing_station_info"
    val stationTable: String = "station_info"
    transformMigration.migration(originAfcStation, targetAfcStation, stationTable)
  }
}
