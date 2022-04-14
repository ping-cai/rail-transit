package afc

import com.alibaba.fastjson.JSON
import config.{HdfsConf, KafkaConf}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}
import sink.HdfsAfcSink

/**
  * afc数据接收器
  */
class AfcReceiver(sparkSession: SparkSession) {

  def this(sparkSession: SparkSession, readOptions: Map[String, String], writeOptions: Map[String, String]) {
    this(sparkSession)
    this.readOptions = readOptions
    this.writeOptions = writeOptions
  }

  private var readOptions: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> KafkaConf.brokers,
    "subscribe" -> "TP_AFC",
    "startingOffsets" -> "latest",
    "enable.auto.commit" -> "true",
    "auto.commit.interval.ms" -> "500",
    "group.id" -> "S_RAIL_TRANSIT_AFC")
  private var writeOptions: Map[String, String] = Map(
    "checkpointLocation" -> s"${HdfsConf.hdfsNamespace}/checkpoint/kafka/TP_AFC",
    "path" -> s"${HdfsConf.hdfsNamespace}/ods/rail_transit/afc_record")

  def consumer(): Dataset[AfcRecord] = {
    import sparkSession.implicits._
    sparkSession
      .readStream
      .format("kafka")
      .options(readOptions)
      .load()
      .selectExpr("CAST (sectionIdList AS STRING) as json")
      .as[String]
      .filter(x => JSON.isValid(x))
      .map(x => JSON.parseObject(x, classOf[AfcRecord]))
  }

  def write(dataStream: Dataset[AfcRecord]): Unit = {
    import sparkSession.implicits._
    dataStream.createOrReplaceTempView("afc_record_stream")
    sparkSession.sql(
      """
        |SELECT reserve,sendingTime sending_time,recordNumber record_number,
        |recordSeq record_seq,ticketId ticket_id,tradingTime trading_time,cardType card_type,
        |transactionEvent transaction_event,stationId station_id,
        |jointTransaction joint_transaction,equipmentNumber equipment_number
        |FROM afc_record_stream
      """.stripMargin)
      .withColumn("trading_date", $"trading_time" cast "date")
      .writeStream
      .format("csv")
      .options(writeOptions)
      .partitionBy("trading_date")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }

  def writeHdfs(dataStream: Dataset[AfcRecord]): Unit = {
    dataStream.createOrReplaceTempView("afc_record_stream")
    dataStream
      .writeStream
      .foreach(
        new HdfsAfcSink(s"${HdfsConf.hdfsNamespace}/ods/rail_transit/afc_record"))
      .outputMode("append")
      .option("checkpointLocation", writeOptions("checkpointLocation"))
      .start()
      .awaitTermination()
  }

  def sinkAfc(): Unit = {
    val afcRecordStream = consumer()
    writeHdfs(afcRecordStream)
  }
}

object AfcReceiver {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("AfcReceiver").getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val afcReceiver = new AfcReceiver(sparkSession)
    afcReceiver.sinkAfc()
  }
}
