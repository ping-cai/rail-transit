import jdbc.OracleConf
import org.apache.spark.sql.SparkSession
import timetable.TravelTimeMigration

object TravelTimeMigrationTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("TravelTimeMigrationTest").getOrCreate()
    val oracleConf = new OracleConf(sparkSession)
    val travelTimeMigration = new TravelTimeMigration(oracleConf)
    travelTimeMigration.migrate()
  }
}
