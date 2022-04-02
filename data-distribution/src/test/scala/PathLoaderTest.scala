import config.HdfsConf
import org.apache.spark.sql.SparkSession
import path.PathLoader

object PathLoaderTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("PathLoaderTest").getOrCreate()
    val hdfsConf = new HdfsConf(sparkSession)
    val pathLoader = new PathLoader(hdfsConf)
    val filePath = "/od/path"
    pathLoader.load(filePath)
  }
}
