package config

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class HdfsConf(val sparkSession: SparkSession) extends Serializable {
  def csv(path: String): DataFrame = {
    sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"${HdfsConf.hdfsNamespace}$path")
  }

  def csvSchema(path: String, schema: StructType): DataFrame = {
    sparkSession
      .read
      .schema(schema)
      .csv(s"${HdfsConf.hdfsNamespace}$path")
  }
}

object HdfsConf {
  val hdfsNamespace: String = Conf.hdfsNamespace

  def getFileSystem: FileSystem = {
    val conf = new Configuration()
    conf.addResource("core-site-client.xml")
    conf.addResource("hdfs-site-client.xml")
    val fileSystem = FileSystem.newInstance(conf)
    fileSystem
  }
}
