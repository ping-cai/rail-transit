import flow.{FlowService, FlowWriter}
import org.apache.spark.sql.SparkSession

object FlowWriterTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("FlowWriter").getOrCreate()
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
