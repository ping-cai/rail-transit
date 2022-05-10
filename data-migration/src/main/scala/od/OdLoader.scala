package od

import config.HdfsConf
import org.apache.spark.sql.Dataset

/**
  * Od数据加载器
  *
  * @param hdfsConf hdfs配置对象
  */
class OdLoader(hdfsConf: HdfsConf) extends Serializable {
  def load(odFilePath: String): Dataset[OdInfo] = {
    val odFrame = hdfsConf.csv(odFilePath)
    val sparkSession = odFrame.sparkSession
    import sparkSession.implicits._
    odFrame.as
  }

  def load(odFilePath: String, granularity: Int): Dataset[OdInfo] = {
    val odFrame = hdfsConf.csv(odFilePath).where(s"granularity=$granularity")
    val sparkSession = odFrame.sparkSession
    import sparkSession.implicits._
    odFrame.as
  }
}
