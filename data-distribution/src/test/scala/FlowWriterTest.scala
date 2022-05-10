import flow.{FlowService, FlowWriter}
import org.apache.spark.sql.SparkSession

object FlowWriterTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("FlowWriter").getOrCreate()
    val distributionService = FlowService.serviceInit(sparkSession)
    val odFilePath = "/dwm/od_record/trading_date=2022-04-02"
    val flowService = new FlowService(distributionService)
    val flowWriter = new FlowWriter(flowService)
    flowWriter.write(odFilePath)
  }
}
