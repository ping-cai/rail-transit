import afc.TransformLoader
import config.HdfsConf
import jdbc.MysqlConf
import od.{OdLoader, OdTransform}
import org.apache.spark.sql.SparkSession
import path.{OdSearchPath, PathSearchService, RoadNetWorkLoader}
import station.StationLoader

object OdSearchPathTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("OdSearchPathTest").master("local[*]").getOrCreate()
    testSearchAllPath(sparkSession)
  }

  private def testSearchPath(sparkSession: SparkSession) = {
    val mysqlConf = new MysqlConf(sparkSession)
    val roadNetWorkLoader = new RoadNetWorkLoader(mysqlConf)
    val pathSearchService = new PathSearchService(roadNetWorkLoader)
    val hdfsConf = new HdfsConf(sparkSession)
    val odLoader = new OdLoader(hdfsConf)
    val transformLoader = new TransformLoader(mysqlConf)
    val odTransform = new OdTransform(odLoader, transformLoader)
    val odSearchPath = new OdSearchPath(pathSearchService, odTransform)
    val odFilePath = "/dwm/od_record/15_minutes/trading_date=2022-03-15"
    val odTransformInfo = odSearchPath.odTransform.transform(odFilePath)
    println(odSearchPath.searchPath(odTransformInfo).first())
  }

  private def testSearchAllPath(sparkSession: SparkSession) = {
    val mysqlConf = new MysqlConf(sparkSession)
    val roadNetWorkLoader = new RoadNetWorkLoader(mysqlConf)
    val pathSearchService = new PathSearchService(roadNetWorkLoader)
    val hdfsConf = new HdfsConf(sparkSession)
    val odLoader = new OdLoader(hdfsConf)
    val transformLoader = new TransformLoader(mysqlConf)
    val odTransform = new OdTransform(odLoader, transformLoader)
    val odSearchPath = new OdSearchPath(pathSearchService, odTransform)
    val stationLoader = new StationLoader(mysqlConf)
    val stationInfo = stationLoader.reload(StationLoader.defaultStation)
    println(odSearchPath.searchAllPath(stationInfo).first())
  }
}
