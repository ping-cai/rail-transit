package path

import od.{OdTransform, OdTransformInfo}
import org.apache.spark.sql.{Dataset, Encoders}
import station.StationInfo

/**
  * Od搜索路径
  *
  * @param pathSearchService 路径搜索接口服务
  * @param odTransform       od编号转换
  */
class OdSearchPath(val pathSearchService: PathSearchService, val odTransform: OdTransform) extends Serializable {

  /**
    * 路径搜索
    *
    * @param odData 转换编号的od数据集
    * @return
    */
  def searchPath(odData: Dataset[OdTransformInfo]): Dataset[PathInfo] = {
    val encoder = Encoders.kryo[PathInfo]
    odData.map(od => {
      val sourceId = od.departureStationId.toString
      val targetId = od.arrivalStationId.toString
      val pathList = pathSearchService.getPathCorrect(sourceId, targetId)
      if (!pathList.isEmpty) {
        path.PathInfo(od.departureTime, od.arrivalTime, pathList, od.passengers)
      } else {
        null
      }
    })(encoder).filter(x => x != null)
  }

  /**
    * 搜索全部Od的路径集合
    *
    * @param stationInfo 车站信息
    * @return
    */
  def searchAllPath(stationInfo: Dataset[StationInfo]): Dataset[SimplePathInfo] = {
    stationInfo.createOrReplaceTempView("station_info")
    val sparkSession = stationInfo.sparkSession
    val encoder = Encoders.javaSerialization[SimplePathInfo]
    val odFrame = sparkSession.sql(
      """
SELECT origin.id source,target.id target
FROM station_info origin
JOIN station_info target
WHERE origin.id <> target.id
LIMIT 10
      """.stripMargin)
    val pathSearchBroadcast = sparkSession.sparkContext.broadcast(pathSearchService)
    odFrame.mapPartitions(partition => {
      partition.map(x => {
        val source = x.getAs[Int]("source").toString
        val target = x.getAs[Int]("target").toString
        try {
          val pathList = pathSearchBroadcast.value.getPathCorrect(source, target)
          SimplePathInfo(source, target, pathList)
        } catch {
          case _: NullPointerException => SimplePathInfo(source, target, null)
        }
      })
    })(encoder)
  }
}
