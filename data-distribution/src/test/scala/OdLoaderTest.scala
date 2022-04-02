import config.HdfsConf
import od.OdLoader
import org.apache.spark.sql.SparkSession

object OdLoaderTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("OdLoaderTest").getOrCreate()
    val hdfsConf = new HdfsConf(sparkSession)
    val odLoader = new OdLoader(hdfsConf)
    val odFilePath = "/dwm/od_record/15_minutes/trading_date=2022-03-15"
    odLoader.load(odFilePath).show()
  }
}
