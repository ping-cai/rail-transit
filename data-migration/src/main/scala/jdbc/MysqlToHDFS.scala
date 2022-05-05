package jdbc

import config.HdfsConf
import org.apache.spark.sql.{SaveMode, SparkSession}

class MysqlToHDFS(mysqlConf: MysqlConf) {
  /**
    * mysql数据导入hdfs中
    */
  def importToHDFS(mysqlTable: String, hdfsPath: String): Unit = {
    val dataFrame = mysqlConf.load(mysqlTable)
    import mysqlConf.sparkSession.implicits._
    val addDateFrame = dataFrame
      .withColumn("trading_date", $"trading_time" cast "date")
    addDateFrame.write.option("header", "true")
      .partitionBy("trading_date")
      .mode(SaveMode.Append).csv(hdfsPath)
  }
}

object MysqlToHDFS {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("jdbc.MysqlToHDFS").getOrCreate()
    val mysqlTable = "afc_record"
    val hdfsPath = s"${HdfsConf.hdfsNamespace}/ods/rail_transit/afc_record/"
    val mysqlConf = new MysqlConf(sparkSession)
    val mysqlToHDFS = new MysqlToHDFS(mysqlConf)
    mysqlToHDFS.importToHDFS(mysqlTable, hdfsPath)
  }
}
