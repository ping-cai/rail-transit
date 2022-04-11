package train

import jdbc.MysqlConf
import org.apache.spark.sql.Dataset

class TrainLoader(mysqlConf: MysqlConf) {
  def load(trainTable: String): Dataset[TrainInfo] = {
    import mysqlConf.sparkSession.implicits._
    mysqlConf.load(trainTable).as
  }
}

object TrainLoader {
  val trainTable = "train"
}
