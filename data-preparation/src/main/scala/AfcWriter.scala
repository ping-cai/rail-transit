import AfcPair.aggregation
import config.HdfsConf
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel

class AfcWriter(afcPair: AfcPair) extends Serializable {
  /**
    * 存储afc所有ETL过程的数据
    *
    */
  def writeOd(): Unit = {
    val odFrame = afcPair.noAggregation()
    val odPath = "/dwd/od_record"
    odFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)
    AfcWriter.write(odFrame, odPath)
    val granularityList = List(15, 30, 60)
    granularityList.foreach(x => {
      val aggFrame = aggregation(x, odFrame)
      val aggRelativePath = s"/dwm/od_record/${x}_minutes"
      AfcWriter.write(aggFrame, aggRelativePath)
    })
  }
}

object AfcWriter {
  /**
    * 配对存储
    *
    * @param dataFrame    存储DF
    * @param relativePath 相对路径
    */
  def write(dataFrame: DataFrame, relativePath: String): Unit = {
    val savePath = s"${HdfsConf.hdfsNamespace}$relativePath"
    dataFrame
      .coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Append)
      .partitionBy("trading_date")
      .csv(savePath)
  }
}
