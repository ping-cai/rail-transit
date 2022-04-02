package path

import config.HdfsConf
import org.apache.spark.sql.Encoders

class PathLoader(hdfsConf: HdfsConf) {
  def load(filePath: String): Unit = {
    val encoder = Encoders.javaSerialization[SimplePathInfo]
    val dataFrame = hdfsConf.csv(filePath).as[SimplePathInfo](encoder)
  }
}
