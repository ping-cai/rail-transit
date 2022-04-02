package od

import config.HdfsConf
import org.apache.spark.sql.Dataset

class OdLoader(hdfsConf: HdfsConf) extends Serializable {
  def load(odFilePath: String): Dataset[OdInfo] = {
    val odFrame = hdfsConf.csv(odFilePath)
    val sparkSession = odFrame.sparkSession
    import sparkSession.implicits._
    odFrame.as
  }
}
