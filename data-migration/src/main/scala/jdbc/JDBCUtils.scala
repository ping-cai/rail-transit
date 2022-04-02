package jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

object JDBCUtils {
  val user: String = MysqlConf.mysqlUser
  val password: String = MysqlConf.mysqlPassword
  val url: String = MysqlConf.mysqlUrl
  Class.forName("com.mysql.cj.jdbc.Driver")

  // 获取连接
  def getConnection: Connection = {
    DriverManager.getConnection(url, user, password)
  }

  // 释放连接
  def closeConnection(connection: Connection, statement: PreparedStatement): Unit = {
    try {
      if (statement != null) {
        statement.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}
