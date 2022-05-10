import config.HdfsConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class AfcExtract(sparkSession: SparkSession, hdfsPath: String = AfcExtract.hdfsPath) {
  private val readPath = s"${HdfsConf.hdfsNamespace}$hdfsPath"

  /**
    * 抽取AFC数据字段
    *
    * @param dataFrame 数据
    * @return
    */
  private def extra(dataFrame: DataFrame): DataFrame = {
    dataFrame.createOrReplaceTempView("afc_record")
    val extraFrame = sparkSession.sql(
      """
SELECT ticket_id,trading_time,transaction_event,station_id,date(trading_time) trading_date
FROM afc_record
      """.stripMargin)
    extraFrame
  }

  /**
    * 读取全部AFC数据进行处理
    *
    * @return
    */
  def extraAllDay(): DataFrame = {
    val dataFrame = sparkSession.read.option("header", "true").csv(readPath)
    extra(dataFrame)
  }

  /**
    * 时间分区读取AFC数据进行处理
    *
    * @param trading_date 交易时间分区
    * @return
    */
  def extraOneDay(trading_date: String): DataFrame = {
    val tradingReadPath = s"$readPath/trading_date=$trading_date"
    val dataFrame = sparkSession.read.option("header", "true").csv(tradingReadPath)
    extra(dataFrame)
  }
}

object AfcExtract {
  val hdfsPath = s"/ods/rail_transit/afc_record"

  def save(extraFrame: DataFrame, relativePath: String): Unit = {
    val savePath = s"${HdfsConf.hdfsNamespace}$relativePath"
    extraFrame.write
      .option("header", "true")
      .mode(SaveMode.Append)
      .partitionBy("trading_date")
      .csv(savePath)
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("AFCExtract").getOrCreate()
    val relativePath = "/dwd/rail_transit/afc_record"
    val aFCExtract = new AfcExtract(sparkSession, hdfsPath)
    if (args.isEmpty) {
      val extraFrame = aFCExtract.extraAllDay()
      save(extraFrame, relativePath)
    } else {
      val extraFrame = aFCExtract.extraOneDay(args(0))
      save(extraFrame, relativePath)
    }
  }
}
