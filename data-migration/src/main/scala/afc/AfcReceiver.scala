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

  /**
    * 读取AFC记录数据的消费者
    *
    * @return 实时AFC记录
    */
  def consumer(): Dataset[AfcRecord] = {
    import sparkSession.implicits._
    sparkSession
      .readStream
      // 读取Kafka数据源
      .format("kafka")
      // 传递读取参数
      .options(readOptions)
      .load()
      // 将字节流转化为字符串
      .selectExpr("CAST (value AS STRING) as json")
      .as[String]
      // 过滤非JSON字符串
      .filter(x => JSON.isValid(x))
      // 将JSON字符串反序列化为AFC对象
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

  /**
    * 将实时数据追加写入HDFS
    *
    * @param dataStream AFC数据流
    */
  def writeHdfs(dataStream: Dataset[AfcRecord]): Unit = {
    dataStream
      // 流式数据写入
      .writeStream
      // 自定义HDFS追加写入方法，因为HDFS不天然支持实时数据写入
      .foreach(
      new HdfsAfcSink(s"${HdfsConf.hdfsNamespace}/ods/rail_transit/afc_record"))
      // 输出模式为追加模式
      .outputMode(OutputMode.Append())
      // 定义检查点位置
      .option("checkpointLocation", writeOptions("checkpointLocation"))
      // 启动一个新线程执行
      .start()
      // 阻塞方法，不间断运行
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
