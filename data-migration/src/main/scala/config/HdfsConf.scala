package config

import org.apache.spark.sql.{DataFrame, SparkSession}

class HdfsConf(sparkSession: SparkSession) extends Serializable {
  def csv(path: String): DataFrame = {
    sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"${HdfsConf.hdfsNamespace}$path")
  }
}

object HdfsConf {
  val hdfsNamespace: String = Conf.hdfsNamespace
}
