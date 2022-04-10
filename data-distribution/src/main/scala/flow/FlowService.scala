package flow

import java.sql.Timestamp

import afc.TransformLoader
import config.HdfsConf
import jdbc.MysqlConf
import od.{OdLoader, OdTransform}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import path.{OdSearchPath, PathSearchService, RoadNetWorkLoader}

import scala.collection.JavaConverters._

/**
  * 流量聚合
  *
  * @param distributionService 客流分配服务
  */
class FlowService(val distributionService: DistributionService) {
  /**
    * 路径流量转换为区间流量
    *
    * @param pathFlowData 路径流量集
    * @return
    */
  def mapSectionFlow(pathFlowData: Dataset[PathFlow]): Dataset[SectionFlow] = {
    val odSearchPath = distributionService.odSearchPath
    val pathSearchService = odSearchPath.pathSearchService
    val roadNetWorkLoader = pathSearchService.roadNetWorkLoader
    val secondMap = roadNetWorkLoader.sectionMap
    val transferMap = roadNetWorkLoader.transferMap
    import pathFlowData.sparkSession.implicits._
    pathFlowData.mapPartitions(partition => {
      partition.flatMap(pathFlow => {
        val departureTime = pathFlow.departureTime
        val path = pathFlow.path
        val edges = path.getEdges
        var startTime = departureTime.getTime
        val sectionFlowBuffer = edges.asScala.map(x => {
          if (secondMap.contains(x)) {
            val seconds = secondMap(x)
            val sectionFlow = SectionFlow(new Timestamp(startTime), new Timestamp(startTime + seconds),
              x.getFromNode, x.getToNode, pathFlow.passengers)
            startTime += seconds
            sectionFlow
          } else {
            val seconds = transferMap(x)
            startTime += seconds
            null
          }
        }).filter(x => x != null)
        sectionFlowBuffer
      })
    })
  }

  /**
    * 路径流量转换为换乘流量
    *
    * @param pathFlowData 路径流量集
    * @return
    */
  def mapTransferFlow(pathFlowData: Dataset[PathFlow]): Dataset[TransferFlow] = {
    val odSearchPath = distributionService.odSearchPath
    val pathSearchService = odSearchPath.pathSearchService
    val roadNetWorkLoader = pathSearchService.roadNetWorkLoader
    val sectionMap = roadNetWorkLoader.sectionMap
    val transferMap = roadNetWorkLoader.transferMap
    val edgeInfoMap = roadNetWorkLoader.edgeInfoMap
    import pathFlowData.sparkSession.implicits._
    pathFlowData.mapPartitions(partition => {
      partition.flatMap(pathFlow => {
        val departureTime = pathFlow.departureTime
        val path = pathFlow.path
        val edges = path.getEdges
        var startTime = departureTime.getTime
        edges.asScala.map(edge => {
          if (sectionMap.contains(edge)) {
            val seconds = sectionMap(edge)
            startTime += seconds
            null
          } else {
            val seconds = transferMap(edge)
            val edgeInfo = edgeInfoMap(edge)
            val transfer_lines = edgeInfo.line.split(" ")
            val transferFlow = TransferFlow(new Timestamp(startTime), new Timestamp(startTime + seconds), edge.getFromNode, edge.getToNode,
              transfer_lines(0), transfer_lines(1), pathFlow.passengers)
            startTime += seconds
            transferFlow
          }
        }).filter(x => x != null)
      })
    })
  }

  /**
    * 路径流量转换为车站流量
    *
    * @param pathFlowData 路径流量集
    * @return
    */
  def mapStationFlow(pathFlowData: Dataset[PathFlow]): Dataset[StationFlow] = {
    val odSearchPath = distributionService.odSearchPath
    val pathSearchService = odSearchPath.pathSearchService
    val roadNetWorkLoader = pathSearchService.roadNetWorkLoader
    val sectionMap = roadNetWorkLoader.sectionMap
    import pathFlowData.sparkSession.implicits._
    pathFlowData.mapPartitions(partition => {
      partition.flatMap(pathFlow => {
        val departureTime = pathFlow.departureTime
        val arrivalTime = pathFlow.arrivalTime
        val path = pathFlow.path
        val edges = path.getEdges
        var startTime = departureTime.getTime
        val endTime = arrivalTime.getTime
        val firstEdge = try {
          edges.getFirst
        } catch {
          case e: Exception =>
            null
        }
        val lastEdge = edges.getLast
        val firstSeconds = sectionMap(firstEdge)
        val firstStationFlow = StationFlow(new Timestamp(startTime), new Timestamp(startTime + firstSeconds),
          firstEdge.getFromNode, pathFlow.passengers, 0)
        val lastSeconds = sectionMap(lastEdge)
        val secondStationFlow = StationFlow(new Timestamp(endTime - lastSeconds), new Timestamp(endTime),
          lastEdge.getToNode, 0, pathFlow.passengers)
        List(firstStationFlow, secondStationFlow)
      })
    })
  }
}

object FlowService {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("FlowService").getOrCreate()
    val distributionService: DistributionService = serviceInit(sparkSession)
    val odFilePath = if (args.isEmpty) {
      "/dwm/od_record/15_minutes"
    } else {
      s"/dwm/od_record/15_minutes/trading_date=${args(0)}"
    }
    val pathFlows = distributionService.distribute(odFilePath)
    val flowService = new FlowService(distributionService)
  }

  def serviceInit(sparkSession: SparkSession) = {
    val mysqlConf = new MysqlConf(sparkSession)
    val roadNetWorkLoader = new RoadNetWorkLoader(mysqlConf)
    val pathSearchService = new PathSearchService(roadNetWorkLoader)
    val hdfsConf = new HdfsConf(sparkSession)
    val odLoader = new OdLoader(hdfsConf)
    val transformLoader = new TransformLoader(mysqlConf)
    val odTransform = new OdTransform(odLoader, transformLoader)
    val odSearchPath = new OdSearchPath(pathSearchService, odTransform)
    val distributionService = new DistributionService(odSearchPath)
    distributionService
  }

  /**
    * 聚合区间流量
    *
    * @param sectionFlowData 区间流量
    * @param granularity     分配粒度
    * @return
    */
  def aggSectionFlow(sectionFlowData: Dataset[SectionFlow], granularity: Int): Dataset[SectionFlow] = {
    import sectionFlowData.sparkSession.implicits._
    val windowDuration = s"$granularity minutes"
    sectionFlowData
      .select($"start_time", $"start_station_id", $"end_station_id", $"flow")
      .groupBy(
        window($"start_time", windowDuration),
        $"start_station_id", $"end_station_id"
      )
      .agg(sum($"flow") as "flow")
      .select($"window.start" as "start_time", $"window.end" as "end_time", $"start_station_id", $"end_station_id", $"flow")
      .as
  }

  /**
    * 聚合换乘流量
    *
    * @param transferFlowData 换乘流量
    * @param granularity      分配粒度
    * @return
    */
  def aggTransferFlow(transferFlowData: Dataset[TransferFlow], granularity: Int): Dataset[TransferFlow] = {
    import transferFlowData.sparkSession.implicits._
    val windowDuration = s"$granularity minutes"
    transferFlowData
      .select($"start_time", $"transfer_out_station_id", $"transfer_in_station_id", $"transfer_out_line", $"transfer_in_line", $"flow")
      .groupBy(
        window($"start_time", windowDuration),
        $"transfer_out_station_id", $"transfer_in_station_id", $"transfer_out_line", $"transfer_in_line"
      )
      .agg(sum($"flow") as "flow")
      .select($"window.start" as "start_time", $"window.end" as "end_time", $"transfer_out_station_id", $"transfer_in_station_id", $"transfer_out_line", $"transfer_in_line", $"flow")
      .as
  }

  /**
    * 聚合车站流量
    *
    * @param stationFlowData 车站流量
    * @param granularity     分配粒度
    * @return
    */
  def aggStationFlow(stationFlowData: Dataset[StationFlow], granularity: Int): Dataset[StationFlow] = {
    import stationFlowData.sparkSession.implicits._
    val windowDuration = s"$granularity minutes"
    stationFlowData
      .select($"start_time", $"station_id", $"in_flow", $"out_flow")
      .groupBy(
        window($"start_time", windowDuration),
        $"station_id"
      )
      .agg(sum($"in_flow") as "in_flow", sum($"out_flow") as "out_flow")
      .select($"window.start" as "start_time", $"window.end" as "end_time", $"station_id", $"in_flow", $"out_flow")
      .as
  }
}
