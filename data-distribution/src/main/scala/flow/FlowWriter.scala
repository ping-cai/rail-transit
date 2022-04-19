package flow

import config.HdfsConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * 流量信息写入hdfs
  *
  * @param flowService 流量转换和聚合服务
  */
class FlowWriter(flowService: FlowService) {
  /**
    * 通过od文件路径直接写入
    *
    * @param odFilePath od文件路径
    */
  def write(odFilePath: String): Unit = {
    val pathFlowData: Dataset[PathFlow] = flowService.distributionService.distribute(odFilePath)
    write(pathFlowData)
  }

  /**
    * 通过数据集进行写入
    *
    * @param pathFlowData 路径流量集
    */
  def write(pathFlowData: Dataset[PathFlow]): Unit = {
    pathFlowData.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val sectionFlow = flowService.mapSectionFlow(pathFlowData)
    val transferFlow = flowService.mapTransferFlow(pathFlowData)
    val stationFlow = flowService.mapStationFlow(pathFlowData)
    import pathFlowData.sparkSession.implicits._
    List(15, 30, 60).foreach(granularity => {
      val aggSectionFlow = FlowService.aggSectionFlow(sectionFlow, granularity)
      val aggTransferFlow = FlowService.aggTransferFlow(transferFlow, granularity)
      val aggStationFlow = FlowService.aggStationFlow(stationFlow, granularity)
      import org.apache.spark.sql.functions.lit
      aggSectionFlow.withColumn("ds", $"start_time".cast("date"))
        .withColumn("granularity", lit(granularity))
        .coalesce(1)
        .write.mode(SaveMode.Append)
        .partitionBy("granularity", "ds")
        .option("header", "true")
        .csv(s"${FlowWriter.defaultSavePath}/section_flow")
      aggTransferFlow.withColumn("ds", $"start_time".cast("date"))
        .coalesce(1)
        .write.mode(SaveMode.Append)
        .partitionBy("granularity", "ds")
        .option("header", "true")
        .csv(s"${FlowWriter.defaultSavePath}/transfer_flow")
      aggStationFlow
        .withColumn("flow", $"in_flow" + $"out_flow")
        .withColumn("ds", $"start_time".cast("date"))
        .coalesce(1)
        .write.mode(SaveMode.Append)
        .partitionBy("granularity", "ds")
        .option("header", "true")
        .csv(s"${FlowWriter.defaultSavePath}/station_flow")
    })
  }
}

object FlowWriter {
  val defaultSavePath = s"${HdfsConf.hdfsNamespace}/ads"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("FlowWriter").getOrCreate()
    val distributionService = FlowService.serviceInit(sparkSession)
    val odFilePath = if (args.isEmpty) {
      "/dwm/od_record/15_minutes"
    } else {
      s"/dwm/od_record/15_minutes/trading_date=${args(0)}"
    }
    val flowService = new FlowService(distributionService)
    val flowWriter = new FlowWriter(flowService)
    flowWriter.write(odFilePath)
  }
}
