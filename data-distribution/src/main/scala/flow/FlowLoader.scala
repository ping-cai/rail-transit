package flow

import config.HdfsConf
import org.apache.spark.sql.Dataset

class FlowLoader(val hdfsConf: HdfsConf) {
  def loadAllSection(): Dataset[SectionFlow] = {
    val sparkSession = hdfsConf.sparkSession
    import sparkSession.implicits._
    val sectionFlowPath = s"${FlowLoader.sectionFlowCatalogue}"
    val sectionFlow = hdfsConf.csv(sectionFlowPath).as[SectionFlow]
    sectionFlow
  }

  def loadAllStation(): Dataset[StationFlow] = {
    val sparkSession = hdfsConf.sparkSession
    import sparkSession.implicits._
    val stationFlowPath = s"${FlowLoader.stationFlowCatalogue}"
    val stationFlow: Dataset[StationFlow] = hdfsConf.csv(stationFlowPath).as[StationFlow]
    stationFlow
  }

  def loadAllTransfer(): Dataset[TransferFlow] = {
    val sparkSession = hdfsConf.sparkSession
    import sparkSession.implicits._
    val transferFlowPath = s"${FlowLoader.transferFlowCatalogue}"
    val transferFlow: Dataset[TransferFlow] = hdfsConf.csv(transferFlowPath).as[TransferFlow]
    transferFlow
  }
}

object FlowLoader {
  val sectionFlowCatalogue = s"/ads/section_flow"
  val stationFlowCatalogue = s"/ads/station_flow"
  val transferFlowCatalogue = s"/ads/transfer_flow"
}