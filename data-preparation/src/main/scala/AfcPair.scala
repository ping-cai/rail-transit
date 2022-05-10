import org.apache.spark.sql.functions.window
import org.apache.spark.sql.{DataFrame, SparkSession}

class AfcPair(sparkSession: SparkSession, afcFrame: DataFrame) {
  /**
    * AFC数据进行配对
    *
    * @return OD非聚合数据
    */
  def noAggregation(): DataFrame = {
    // 创建AFC数据的临时视图
    afcFrame.createOrReplaceTempView("afc_extra")
    // 提取顺序出站数据
    val inStationFrame = sparkSession.sql(
      """
SELECT ticket_id,row_number() over(PARTITION BY ticket_id ORDER BY trading_time) ticket_seq,trading_time,station_id,trading_date
FROM afc_extra where transaction_event=21
      """.stripMargin)
    // 提取顺序进站数据
    val outStationFrame = sparkSession.sql(
      """
SELECT ticket_id,row_number() over(PARTITION BY ticket_id ORDER BY trading_time) ticket_seq,trading_time,station_id,trading_date
FROM afc_extra where transaction_event=22
      """.stripMargin)
    // 创建进站数据临时视图
    inStationFrame.createOrReplaceTempView("in_station_record")
    // 创建出站数据临时视图
    outStationFrame.createOrReplaceTempView("out_station_record")
    // 进站数据与出站数据关联
    val odFrame = sparkSession.sql(
      """
    SELECT in_station_record.ticket_id,in_station_record.trading_time in_station_time,
    in_station_record.station_id in_station_id,
    out_station_record.trading_time out_station_time,
    out_station_record.station_id out_station_id,
    |in_station_record.trading_date
    FROM in_station_record
    JOIN out_station_record
    ON in_station_record.ticket_id=out_station_record.ticket_id
    AND in_station_record.ticket_seq=out_station_record.ticket_seq
      """.stripMargin)
    odFrame
  }
}

object AfcPair {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("AFCPair").getOrCreate()
    val afcExtract = new AfcExtract(sparkSession)
    val extraFrame = if (args.isEmpty) {
      afcExtract.extraAllDay()
    } else {
      afcExtract.extraOneDay(args(0))
    }
    val afcPair = new AfcPair(sparkSession, extraFrame)
    val afcWriter = new AfcWriter(afcPair)
    afcWriter.writeOd()
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
