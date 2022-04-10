package path

import java.util

import algorithm.Yen
import config.DistributionConf
import domain.Path
import scala.collection.JavaConverters._

/**
  * 路径搜索和修正
  *
  * @param roadNetWorkLoader 路网图实例
  * @param pathNum           路径数
  * @param stopStationTime   停站时间
  */
class PathSearchService(val roadNetWorkLoader: RoadNetWorkLoader,
                        pathNum: Int = DistributionConf.pathNum,
                        stopStationTime: Int = DistributionConf.stopStationTime) extends Serializable {

  /**
    * K短路计算
    *
    * @param source  进站，源地址
    * @param target  出站，目的地址
    * @param pathNum K短路参数
    * @return K短路List<Path> 集合
    */
  @throws[CloneNotSupportedException]
  def getPathList(source: String, target: String, pathNum: Int): util.List[Path] =
    Yen.ksp(roadNetWorkLoader.graph, source, target, pathNum)

  /**
    * 默认参数K短路计算
    *
    * @param source 进站，源地址
    * @param target 出站，目的地址
    * @return K短路List<Path> 集合
    */
  @throws[CloneNotSupportedException]
  def getPathList(source: String, target: String): util.List[Path] =
    Yen.ksp(roadNetWorkLoader.graph, source, target, pathNum)

  /**
    * 添加停站时间的K短路计算
    * 过滤空路径
    *
    * @param source 进站，源地址
    * @param target 出站，目的地址
    * @return K短路List<Path> 集合
    */
  @throws[CloneNotSupportedException]
  @throws[NullPointerException]
  def getPathCorrect(source: String, target: String): util.List[Path] = {
    val transferMap = roadNetWorkLoader.transferMap
    val paths = Yen.ksp(roadNetWorkLoader.graph, source, target, pathNum)
    paths.asScala.map(path => {
      val edges = path.getEdges
      var totalCost = path.getTotalCost
      while (!edges.isEmpty && transferMap.contains(edges.getFirst)) {
        val edge = edges.removeFirst()
        totalCost -= edge.getWeight
      }
      while (!edges.isEmpty && transferMap.contains(edges.getLast)) {
        val edge = edges.removeLast()
        totalCost -= edge.getWeight
      }
      if (edges.isEmpty) {
        null
      } else {
        val stopNum = edges.size() - 1
        path.setTotalCost(path.getTotalCost + stopNum * stopStationTime)
        path
      }
    }).filter(x => x != null).asJava
  }
}
