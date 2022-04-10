import afc.TransformLoader
import config.HdfsConf
import jdbc.MysqlConf
import od.{OdLoader, OdTransform}
import org.apache.spark.sql.SparkSession
import path.{OdSearchPath, PathInfo, PathSearchService, RoadNetWorkLoader}
import station.StationLoader

object OdSearchPathTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("OdSearchPathTest").master("local[*]").getOrCreate()
    testSearchPath(sparkSession)
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
    val odFilePath = "/dwm/od_record/60_minutes/trading_date=2022-04-02"
    val odTransformInfo = odSearchPath.odTransform.transform(odFilePath)
    odSearchPath.searchPath(odTransformInfo).foreachPartition(
      (partition: Iterator[PathInfo]) => {
        partition.toList.foreach(x => println(x))
      }
    )

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
