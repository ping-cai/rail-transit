package jdbc

import config.HdfsConf

class HDFSToMysql(hdfsConf: HdfsConf) {
  def importToMysql(hdfsPath: String, sql: String): Unit = {
    val dataFrame = hdfsConf.csv(hdfsPath)
  }

  def importSection(hdfsPath: String): Unit = {

  }
}
