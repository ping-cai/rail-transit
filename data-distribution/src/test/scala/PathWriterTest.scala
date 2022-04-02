import afc.TransformLoader
import config.HdfsConf
import jdbc.MysqlConf
import od.{OdLoader, OdTransform}
import org.apache.spark.sql.SparkSession
import path.{OdSearchPath, PathSearchService, PathWriter, RoadNetWorkLoader}
import station.StationLoader

object PathWriterTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("OdSearchPathTest").master("local[*]").getOrCreate()
    val mysqlConf = new MysqlConf(sparkSession)
    val roadNetWorkLoader = new RoadNetWorkLoader(mysqlConf)
    val pathSearchService = new PathSearchService(roadNetWorkLoader)
    val hdfsConf = new HdfsConf(sparkSession)
    val odLoader = new OdLoader(hdfsConf)
    val transformLoader = new TransformLoader(mysqlConf)
    val odTransform = new OdTransform(odLoader, transformLoader)
    val odSearchPath = new OdSearchPath(pathSearchService, odTransform)
    val stationLoader = new StationLoader(mysqlConf)
    val pathWriter = new PathWriter(stationLoader, odSearchPath)
    val savePath = "/od/path"
    pathWriter.write(savePath)
  }
}
