import jdbc.MysqlConf
import org.apache.spark.sql.SparkSession
import path.{PathSearchService, RoadNetWorkLoader}

object PathServiceTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("PathServiceTest").getOrCreate()
    val mysqlConf = new MysqlConf(sparkSession)
    val roadNetWorkLoader = new RoadNetWorkLoader(mysqlConf)
    val pathService = new PathSearchService(roadNetWorkLoader)
    val paths = pathService.getPathCorrect("1","2")
    println(paths)
  }
}
