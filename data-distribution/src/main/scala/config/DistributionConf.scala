package config

import com.typesafe.config.{Config, ConfigFactory}

object DistributionConf {
  private val config: Config = ConfigFactory.load("distribution.conf")
  val transferPenalties: Int = config.getInt("transfer.penalties.time.seconds")
  val transferTime: Int = config.getInt("transfer.time.seconds")
  val stopStationTime: Int = config.getInt("stop.station.time.seconds")
  val pathNum: Int = config.getInt("path.search.num")
  // 路权分配参数
  val theta: Double = config.getDouble("theta")
  val alpha: Double = config.getDouble("alpha")
  val beta: Double = config.getDouble("beta")
}
