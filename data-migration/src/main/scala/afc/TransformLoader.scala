package afc

import jdbc.MysqlConf
import org.apache.spark.sql.Dataset

class TransformLoader(mysqlConf: MysqlConf) extends Serializable {
  def load(transformTable: String): Dataset[TransformInfo] = {
    val transformFrame = mysqlConf.load(transformTable)
    import transformFrame.sparkSession.implicits._
    transformFrame.select("station_id", "afc_id").as
  }
}
