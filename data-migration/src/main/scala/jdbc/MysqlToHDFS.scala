package jdbc

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import config.HdfsConf
import org.apache.spark.sql.{SaveMode, SparkSession}

class MysqlToHDFS(sparkSession: SparkSession, mysqlTable: String, hdfsPath: String) {
  /**
    * mysql数据导入hdfs中
    */
  def importToHDFS(): Unit = {
    val dataFrame = sparkSession.read.jdbc(MysqlConf.mysqlUrl, mysqlTable, MysqlConf.prop)
    import sparkSession.implicits._
    val addDateFrame = dataFrame.withColumn("trading_date", $"trading_time" cast "date")
    addDateFrame.write.option("header", "true")
      .partitionBy("trading_date")
      .mode(SaveMode.Append).csv(hdfsPath)
  }
}

object MysqlToHDFS {
  def main(args: Array[String]): Unit = {
    //    val sparkSession = SparkSession.builder().master("local[*]").appName("jdbc.MysqlToHDFS").getOrCreate()
    val sparkSession = SparkSession.builder().appName("jdbc.MysqlToHDFS").getOrCreate()
    val mysqlTable = "afc_record"
    val hdfsPath = s"${HdfsConf.hdfsNamespace}/ods/rail_transit/afc_record/"
    val mysqlToHDFS = new MysqlToHDFS(sparkSession, mysqlTable, hdfsPath)
    mysqlToHDFS.importToHDFS()
  }
}
