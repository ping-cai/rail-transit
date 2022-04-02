import jdbc.MysqlConf
import org.apache.spark.sql.SparkSession
import path.RoadNetWorkLoader

object RoadNetWorkLoaderTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("RoadNetWorkLoaderTest").master("local[*]").getOrCreate()
    val mysqlConf = new MysqlConf(sparkSession)
    val roadNetWorkLoader = new RoadNetWorkLoader(mysqlConf)
    //    println(roadNetWorkLoader.loadActual())
    //    println(roadNetWorkLoader.loadVirtual())
    println(roadNetWorkLoader.graph)
  }
}
