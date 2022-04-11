package train

import jdbc.MysqlConf
import org.apache.spark.sql.Dataset

case class TrainOperationLoader(mysqlConf: MysqlConf) {
  def load(): Dataset[TrainOperationInfo] = {
    val dataFrame = mysqlConf.load(TrainOperationLoader.trainOperationTable)
    val sparkSession = dataFrame.sparkSession
    import sparkSession.implicits._
    dataFrame.as
  }
}

object TrainOperationLoader {
  val trainOperationTable = "train_operation"
}
