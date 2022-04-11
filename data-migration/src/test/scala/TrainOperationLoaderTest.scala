import jdbc.MysqlConf
import org.apache.spark.sql.SparkSession
import train.TrainOperationLoader

object TrainOperationLoaderTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("TrainOperationLoaderTest").getOrCreate()
    val mysqlConf = new MysqlConf(sparkSession)
    val trainOperationLoader = new TrainOperationLoader(mysqlConf)
    trainOperationLoader.load()
  }
}
