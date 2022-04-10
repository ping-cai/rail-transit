package flow

import config.DistributionConf
import domain.Path
import org.apache.spark.sql.{Dataset, Encoders}
import path.{OdSearchPath, PathInfo}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * 客流分配服务
  *
  * @param odSearchPath od路径搜索
  */
class DistributionService(val odSearchPath: OdSearchPath) {
  /**
    * 通过od文件进行路径搜索和流量分配
    *
    * @param odFilePath od文件路径
    * @return
    */
  def distribute(odFilePath: String): Dataset[PathFlow] = {
    val odTransform = odSearchPath.odTransform.transform(odFilePath)
    val pathInfo = odSearchPath.searchPath(odTransform)
    DistributionService.distribute(pathInfo)
  }
}

object DistributionService {
  val exp: Double = Math.E
  val theta: Double = DistributionConf.theta

  /**
    * 通过路径集合得到流量分配集合
    *
    * @param pathInfo 路径集合信息
    * @return
    */
  def distribute(pathInfo: Dataset[PathInfo]): Dataset[PathFlow] = {
    val encoders = Encoders.kryo[PathFlow]
    pathInfo.mapPartitions(partition => {
      partition.flatMap(pathInfo => {
        val pathList = pathInfo.pathList
        val passengers = pathInfo.passengers
        val logitResult = logit(pathList, passengers)
        logitResult.map(x => {
          val path = x._1
          val passengers = x._2
          PathFlow(pathInfo.departureTime, pathInfo.arrivalTime, path, passengers)
        })
      })
    })(encoders)
  }

  /**
    * logit分配模型具体实现
    *
    * @param pathList   路径搜索结果
    * @param passengers 客流量
    * @return
    */
  def logit(pathList: java.util.List[Path], passengers: Int): mutable.Buffer[(Path, Double)] = {
    val minCost = pathList.stream().mapToDouble(x => x.getTotalCost).min().getAsDouble
    val logitDenominator = pathList.stream().mapToDouble(path => {
      val cost = path.getTotalCost
      val partCost = Math.pow(exp, (-theta) * (cost / minCost))
      partCost
    }).sum()
    pathList.asScala.map(path => {
      val cost = path.getTotalCost
      val molecule = Math.pow(exp, (-theta) * (cost / minCost)) / logitDenominator
      val proportion = molecule * passengers
      (path, proportion)
    })
  }
}
