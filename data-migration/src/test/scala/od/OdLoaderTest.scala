package od

import config.HdfsConf
import jdbc.MysqlConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.date_add

object OdLoaderTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("OdLoaderTest").getOrCreate()
    import sparkSession.implicits._
    val hdfsConf = new HdfsConf(sparkSession)
    val odLoader = new OdLoader(hdfsConf)
    val odFilePath = "/dwm/od_record"
    val odFrame = odLoader.load(odFilePath)
      .where("trading_date='2022-04-02'")
    odFrame
      .withColumn("trading_date", date_add($"trading_date", 1))
      .write
      .mode(SaveMode.Append)
      .jdbc(MysqlConf.mysqlUrl, "od_flow", MysqlConf.prop)
  }
}
