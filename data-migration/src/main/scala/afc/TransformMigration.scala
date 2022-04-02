package afc

import station.StationLoader

class TransformMigration(loader: AfcStationLoader, writer: AfcStationWriter, reLoader: StationLoader) {
  def migration(originAfcStation: String, targetAfcStation: String, stationTable: String): Unit = {
    val afcStationInfo = loader.load(originAfcStation)
    writer.insert(afcStationInfo)
    val stationInfo = reLoader.reload(stationTable)
    writer.insertTransform(afcStationInfo, stationInfo)
  }
}
