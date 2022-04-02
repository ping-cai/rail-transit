package jdbc

import java.util.Properties

import config.Conf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class MysqlConf(sparkSession: SparkSession) extends Serializable {
  def load(tableName: String): DataFrame = {
    sparkSession
      .read
      .option("header,", "true")
      .jdbc(MysqlConf.mysqlUrl, tableName, MysqlConf.prop)
  }
}

object MysqlConf {
  val mysqlUrl: String = Conf.mysqlUrl
  val mysqlUser: String = Conf.mysqlUser
  val mysqlPassword: String = Conf.mysqlPassword
  val prop: Properties = new Properties()
  prop.put("user", mysqlUser)
  prop.put("password", mysqlPassword)

  def overWrite[T](tableName: String, data: Dataset[T]): Unit = {
    data.write
      .mode(SaveMode.Overwrite)
      .jdbc(MysqlConf.mysqlUrl, tableName, MysqlConf.prop)
  }
}
