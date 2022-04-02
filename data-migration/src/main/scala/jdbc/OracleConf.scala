package jdbc

import java.util.Properties

import config.Conf
import org.apache.spark.sql.{DataFrame, SparkSession}

class OracleConf(sparkSession: SparkSession) {
  def load(tableName: String): DataFrame = {
    sparkSession
      .read
      .option("header,", "true")
      .jdbc(OracleConf.url, tableName, OracleConf.prop)
  }
}

object OracleConf {
  val url: String = Conf.oracleUrl
  val user: String = Conf.oracleUser
  val password: String = Conf.oraclePassword
  val prop: Properties = new Properties()
  prop.put("user", user)
  prop.put("password", password)
}
