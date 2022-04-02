import afc.TransformLoader
import config.HdfsConf
import flow.DistributionService
import jdbc.MysqlConf
import od.{OdLoader, OdTransform}
import org.apache.spark.sql.SparkSession
import path.{OdSearchPath, PathSearchService, RoadNetWorkLoader}

object DistributionServiceTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("DistributeFlow").getOrCreate()
    val mysqlConf = new MysqlConf(sparkSession)
    val roadNetWorkLoader = new RoadNetWorkLoader(mysqlConf)
    val pathSearchService = new PathSearchService(roadNetWorkLoader)
    val hdfsConf = new HdfsConf(sparkSession)
    val odLoader = new OdLoader(hdfsConf)
    val transformLoader = new TransformLoader(mysqlConf)
    val odTransform = new OdTransform(odLoader, transformLoader)
    val odSearchPath = new OdSearchPath(pathSearchService, odTransform)
    val distributeFlow = new DistributionService(odSearchPath)
    val odFilePath = "/dwm/od_record/15_minutes/trading_date=2022-03-15"
    val pathFlows = distributeFlow.distribute(odFilePath).collect()
    println(pathFlows)
  }
}
