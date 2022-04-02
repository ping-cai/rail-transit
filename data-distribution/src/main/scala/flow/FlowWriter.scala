package flow

import config.HdfsConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

class FlowWriter(flowService: FlowService) {
  def write(odFilePath: String): Unit = {
    val pathFlowData: Dataset[PathFlow] = flowService.distributionService.distribute(odFilePath)
    write(pathFlowData)
  }

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

      aggSectionFlow.withColumn("ds", $"start_time".cast("date"))
        .coalesce(1)
        .write.mode(SaveMode.Append)
        .partitionBy("ds")
        .csv(s"${FlowWriter.defaultSavePath}/section_flow/$granularity")
      aggTransferFlow.withColumn("ds", $"start_time".cast("date"))
        .coalesce(1)
        .write.mode(SaveMode.Append)
        .partitionBy("ds")
        .csv(s"${FlowWriter.defaultSavePath}/transfer_flow/$granularity")
      aggStationFlow.withColumn("ds", $"start_time".cast("date"))
        .coalesce(1)
        .write.mode(SaveMode.Append)
        .partitionBy("ds")
        .csv(s"${FlowWriter.defaultSavePath}/station_flow/$granularity")
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
