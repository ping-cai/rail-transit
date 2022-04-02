import jdbc.{MysqlConf, OracleConf}
import org.apache.spark.sql.SparkSession
import station.StationLoader

object StationLoaderTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("StationLoaderTest").getOrCreate()
    val mysqlConf = new MysqlConf(sparkSession)
    val oracleConf = new OracleConf(sparkSession)
    val stationLoader = new StationLoader(oracleConf, mysqlConf)
    //    val originStation = "SCOTT.\"2-yxtStation\""
    val targetStation = "station_info"
    stationLoader.reload(targetStation)
  }
}
