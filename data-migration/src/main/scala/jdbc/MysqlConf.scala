package jdbc

import java.util.Properties

import config.Conf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * MySQL功能类
  *
  * @param sparkSession Spark会话对象
  */
class MysqlConf(val sparkSession: SparkSession) extends Serializable {
  /**
    * 通过SparkAPI读取MySQL的表
    *
    * @param tableName 表名
    * @return
    */
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
}
