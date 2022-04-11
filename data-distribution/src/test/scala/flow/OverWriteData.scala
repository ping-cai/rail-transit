package flow

import config.HdfsConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}

object OverWriteData {
  val sectionSchema: StructType = new StructType()
    .add("start_time", DataTypes.TimestampType, nullable = true)
    .add("end_time", DataTypes.TimestampType, nullable = true)
    .add("start_station_id", DataTypes.StringType, nullable = true)
    .add("end_station_id", DataTypes.StringType, nullable = true)
    .add("flow", DataTypes.DoubleType, nullable = true)
  val sectionFlowPath = "/ads/section_flow"

  def readAndSave(hdfsConf: HdfsConf, readPath: String, granularity: Int): Unit = {
    val dataFrame = hdfsConf.csvSchema(readPath, sectionSchema)
    import org.apache.spark.sql.functions._
    dataFrame.withColumn("granularity", lit(granularity))
      .write
      .option("header", "true")
      .partitionBy("granularity", "ds")
      .mode(SaveMode.Overwrite)
      .csv(s"${HdfsConf.hdfsNamespace}/$sectionFlowPath")
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("OverWriteData").getOrCreate()
    val hdfsConf = new HdfsConf(sparkSession)
    List(15, 30, 60).foreach(x => {
      readAndSave(hdfsConf, s"$sectionFlowPath/$x", x)
    })
  }
}
