import AfcPair.aggregation
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class AfcPair(sparkSession: SparkSession, afcFrame: DataFrame) {
  /**
    * 原始无聚合数据
    *
    * @return
    */
  def noAggregation(): DataFrame = {
    afcFrame.createOrReplaceTempView("afc_extra")
    val inStationFrame = sparkSession.sql(
      """
SELECT ticket_id,row_number() over(PARTITION BY ticket_id ORDER BY trading_time) ticket_seq,trading_time,station_id,trading_date
FROM afc_extra where transaction_event=21
      """.stripMargin)

    val outStationFrame = sparkSession.sql(
      """
SELECT ticket_id,row_number() over(PARTITION BY ticket_id ORDER BY trading_time) ticket_seq,trading_time,station_id,trading_date
FROM afc_extra where transaction_event=22
      """.stripMargin)
    inStationFrame.createOrReplaceTempView("in_station_record")
    outStationFrame.createOrReplaceTempView("out_station_record")
    val odFrame = sparkSession.sql(
      """
SELECT in_station_record.ticket_id,in_station_record.trading_time in_station_time,in_station_record.station_id in_station_id,
out_station_record.trading_time out_station_time,out_station_record.station_id out_station_id,
|in_station_record.trading_date
FROM in_station_record JOIN out_station_record ON in_station_record.ticket_id=out_station_record.ticket_id
AND in_station_record.ticket_seq=out_station_record.ticket_seq
      """.stripMargin)
    odFrame
  }

  /**
    * 存储afc所有ETL过程的数据
    *
    */
  def save(): Unit = {
    val aFCPair = new AfcPair(sparkSession, afcFrame)
    val odFrame = aFCPair.noAggregation()
    val odPath = "/dwd/od_record"
    odFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val afcWriter = new AfcWriter()
    afcWriter.write(odFrame, odPath)
    val granularityList = List(15, 30, 60)
    granularityList.foreach(x => {
      val aggFrame = aggregation(x, odFrame)
      val aggRelativePath = s"/dwm/od_record/${x}_minutes"
      afcWriter.write(aggFrame, aggRelativePath)
    })
  }
}

object AfcPair {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("AFCPair").getOrCreate()
    val afcExtract = new AfcExtract(sparkSession)
    val extraFrame = if (args.isEmpty) {
      afcExtract.read()
    } else {
      afcExtract.read(args(0))
    }
    val afcPair = new AfcPair(sparkSession, extraFrame)
    afcPair.save()
  }

  /**
    * 聚合粒度分钟级别
    *
    * @param granularity 聚合粒度
    */
  def aggregation(granularity: Int, dataFrame: DataFrame): DataFrame = {
    dataFrame.createOrReplaceTempView("od_record")
    import dataFrame.sparkSession.implicits._
    val windowDuration = s"$granularity minutes"
    val odAggregation = dataFrame
      .groupBy(window($"in_station_time", windowDuration), $"in_station_id", $"out_station_id", $"trading_date")
      .count()
    val odAggFrame = odAggregation.select($"window.start", $"window.end", $"in_station_id", $"out_station_id", $"count" as "passengers", $"trading_date")
    odAggFrame
  }


}
