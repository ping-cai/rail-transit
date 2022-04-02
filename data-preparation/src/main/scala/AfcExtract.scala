import config.HdfsConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class AfcExtract(sparkSession: SparkSession, hdfsPath: String = AfcExtract.hdfsPath) {
  private val readPath = s"${HdfsConf.hdfsNamespace}$hdfsPath"

  def read(): DataFrame = {
    val dataFrame = sparkSession.read.option("header", "true").csv(readPath)
    dataFrame.createOrReplaceTempView("afc_record")
    val extraFrame = sparkSession.sql(
      """
SELECT ticket_id,trading_time,transaction_event,station_id,date(trading_time) trading_date
FROM afc_record
      """.stripMargin)
    extraFrame
  }

  def read(trading_date: String): DataFrame = {
    val dataFrame = sparkSession.read.option("header", "true").csv(readPath)
      .where(s"trading_date='$trading_date'")
    dataFrame.createOrReplaceTempView("afc_record")
    val extraFrame = sparkSession.sql(
      s"""
SELECT ticket_id,trading_time,transaction_event,station_id,date(trading_time) trading_date
FROM afc_record
      """.stripMargin)
    extraFrame
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
      val extraFrame = aFCExtract.read()
      save(extraFrame, relativePath)
    } else {
      val extraFrame = aFCExtract.read(args(0))
      save(extraFrame, relativePath)
    }
  }
}
