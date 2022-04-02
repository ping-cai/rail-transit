package od

import afc.{TransformInfo, TransformLoader}
import org.apache.spark.sql.Dataset

class OdTransform(odLoader: OdLoader, transformLoader: TransformLoader) extends Serializable {
  def transform(odFilePath: String, transformTable: String = OdTransform.transformTable): Dataset[OdTransformInfo] = {
    val originOdFrame: Dataset[OdInfo] = odLoader.load(odFilePath)
    val transformFrame: Dataset[TransformInfo] = transformLoader.load(transformTable)
    OdTransform.transform(originOdFrame, transformFrame)
  }
}

object OdTransform {
  val transformTable = "station_transform"

  def transform(originOdFrame: Dataset[OdInfo], transformFrame: Dataset[TransformInfo]): Dataset[OdTransformInfo] = {
    originOdFrame.createOrReplaceTempView("od")
    transformFrame.createOrReplaceTempView("station_transform")
    val sparkSession = originOdFrame.sparkSession
    import sparkSession.implicits._
    sparkSession.sql(
      """
        |SELECT start departureTime,end arrivalTime,
        |target1.station_id departureStationId,target2.station_id arrivalStationId,passengers
        |FROM od origin
        |JOIN station_transform target1 ON origin.in_station_id=target1.afc_id
        |JOIN station_transform target2 ON origin.out_station_id=target2.afc_id
      """.stripMargin).as
  }
}
