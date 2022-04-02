package config

import com.typesafe.config.{Config, ConfigFactory}

object Conf {
  private val config: Config = ConfigFactory.load("migration.conf")
  val mysqlUrl: String = config.getString("mysql.url")
  val mysqlUser: String = config.getString("mysql.user")
  val mysqlPassword: String = config.getString("mysql.password")
  val hdfsNamespace: String = config.getString("hdfs.namespace")
  val oracleUrl: String = config.getString("oracle.url")
  val oracleUser: String = config.getString("oracle.user")
  val oraclePassword: String = config.getString("oracle.password")
  val stationTable: String = config.getString("station.table.name")
  val sectionTable: String = config.getString("section.table.name")
}
