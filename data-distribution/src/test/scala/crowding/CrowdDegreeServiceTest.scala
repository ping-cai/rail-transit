package crowding

import config.HdfsConf
import flow.FlowLoader
import jdbc.MysqlConf
import org.apache.spark.sql.SparkSession
import section.SectionLoader
import train.{TrainLoader, TrainOperationLoader}

object CrowdDegreeServiceTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("CrowdDegreeServiceTest").getOrCreate()
    val hdfsConf = new HdfsConf(sparkSession)
    val flowLoader = new FlowLoader(hdfsConf)
    val mysqlConf = new MysqlConf(sparkSession)
    val sectionLoader = new SectionLoader(mysqlConf)
    val trainLoader = new TrainLoader(mysqlConf)
    val trainOperationLoader = new TrainOperationLoader(mysqlConf)
    val crowdDegreeService = new CrowdDegreeService(sectionLoader, trainLoader, trainOperationLoader)
    val sectionFlow = flowLoader.loadAllSection()
    crowdDegreeService.setCrowdDegree(sectionFlow)
  }
}
