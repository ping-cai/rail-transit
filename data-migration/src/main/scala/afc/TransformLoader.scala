package afc

import jdbc.MysqlConf
import org.apache.spark.sql.Dataset

/**
  * Od编号转换表加载
  *
  * @param mysqlConf mysql配置对象
  */
class TransformLoader(mysqlConf: MysqlConf) extends Serializable {
  /**
    * 加载系统编号转换表
    *
    * @param transformTable 表名
    * @return
    */
  def load(transformTable: String): Dataset[TransformInfo] = {
    val transformFrame = mysqlConf.load(transformTable)
    import transformFrame.sparkSession.implicits._
    transformFrame.select("station_id", "afc_id").as
  }
}
