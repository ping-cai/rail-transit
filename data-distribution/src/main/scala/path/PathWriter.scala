package path

import config.HdfsConf
import org.apache.spark.sql.SaveMode
import station.StationLoader

class PathWriter(stationLoader: StationLoader, odSearchPath: OdSearchPath) {
  def write(savePath: String): Unit = {
    val stationInfo = stationLoader.reload(StationLoader.defaultStation)
    val allPathInfo = odSearchPath.searchAllPath(stationInfo)
    allPathInfo.write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(s"${HdfsConf.hdfsNamespace}$savePath")
  }
}
