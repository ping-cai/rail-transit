import config.HdfsConf
import org.apache.spark.sql.{DataFrame, SaveMode}

class AfcWriter extends Serializable {
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
