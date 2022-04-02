package station

import jdbc.OracleConf
import org.apache.spark.sql.SparkSession

/**
  * 车站数据迁移
  */
class StationMigration(loader: StationLoader, writer: StationWriter) {
  def migration(originStation: String, targetStation: String): Unit = {
    val stationInfo = loader.load(originStation)
    writer.insert(stationInfo)
  }
}

object StationMigration {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("StationMigration").getOrCreate()
    val oracleConf = new OracleConf(sparkSession)
    val stationLoader = new StationLoader(oracleConf)
    val stationWriter = new StationWriter
    val stationMigration = new StationMigration(stationLoader, stationWriter)
    val originStation = "SCOTT.\"2-yxtStation\""
    val targetStation = "station_info"
    stationMigration.migration(originStation, targetStation)
  }
}
