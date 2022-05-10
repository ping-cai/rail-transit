package jdbc

import config.HdfsConf
import org.apache.spark.sql.{SaveMode, SparkSession}

class MysqlToHDFS(mysqlConf: MysqlConf) {
  /**
    * mysql数据导入hdfs中
    */
  def importToHDFS(mysqlTable: String, hdfsPath: String): Unit = {
    // 加载MySQL中的数据，分布式读取
    val dataFrame = mysqlConf.load(mysqlTable)
    // 隐式转换，使得$符号可以提取列信息
    import mysqlConf.sparkSession.implicits._
    val addDateFrame = dataFrame
      // 添加分区列
      .withColumn("trading_date", $"trading_time" cast "date")
    // 将dataFrame输出
    addDateFrame
      .write
      // 添加表头
      .option("header", "true")
      // 分区列名
      .partitionBy("trading_date")
      // 输出模式
      .mode(SaveMode.Append)
      // 输出文件形式
      .csv(hdfsPath)
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
