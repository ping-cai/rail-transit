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
    val pathFlowData: Dataset[PathFlow] = flowService.
      distributionService.distribute(odFilePath)
    write(pathFlowData)
  }

  /**
    * 通过数据集进行写入
    *
    * @param pathFlowData 路径流量集
    */
  def write(pathFlowData: Dataset[PathFlow]): Unit = {
    // 数据集缓存策略，内存序列化和磁盘序列化
    pathFlowData.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // 转换路径流量为区间断面流量
    val sectionFlow = flowService.mapSectionFlow(pathFlowData)
    // 转换路径流量为换乘流量
    val transferFlow = flowService.mapTransferFlow(pathFlowData)
    // 转换路径流量为车站流量
    val stationFlow = flowService.mapStationFlow(pathFlowData)
    // 导入隐式转换
    import pathFlowData.sparkSession.implicits._
    // 客流粒度遍历
    List(15, 30, 60).foreach(granularity => {
      // 聚合区间流量
      val aggSectionFlow = FlowService.aggSectionFlow(sectionFlow, granularity)
      // 聚合换乘流量
      val aggTransferFlow = FlowService.aggTransferFlow(transferFlow, granularity)
      // 聚合车站流量
      val aggStationFlow = FlowService.aggStationFlow(stationFlow, granularity)
      import org.apache.spark.sql.functions.lit
      aggSectionFlow
        // 添加天分区键
        .withColumn("ds", $"start_time".cast("date"))
        // 添加客流粒度分区键
        .withColumn("granularity", lit(granularity))
        // 分区数量从200降低为1
        .coalesce(1)
        // 追加写入模式
        .write.mode(SaveMode.Append)
        // 分区操作
        .partitionBy("ds", "granularity")
        // 添加表头
        .option("header", "true")
        // 写入格式和地址
        .csv(s"${FlowWriter.defaultSavePath}/section_flow")
      aggTransferFlow.withColumn("ds", $"start_time".cast("date"))
        .coalesce(1)
        .write.mode(SaveMode.Append)
        .partitionBy("ds", "granularity")
        .option("header", "true")
        .csv(s"${FlowWriter.defaultSavePath}/transfer_flow")
      aggStationFlow
        .withColumn("flow", $"in_flow" + $"out_flow")
        .withColumn("ds", $"start_time".cast("date"))
        .coalesce(1)
        .write.mode(SaveMode.Append)
        .partitionBy("ds", "granularity")
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
      "/dwm/od_record/"
    } else {
      s"/dwm/od_record/trading_date=${args(0)}"
    }
    val flowService = new FlowService(distributionService)
    val flowWriter = new FlowWriter(flowService)
    flowWriter.write(odFilePath)
  }
}
