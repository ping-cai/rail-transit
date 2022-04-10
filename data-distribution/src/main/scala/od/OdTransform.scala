package od

import afc.{TransformInfo, TransformLoader}
import org.apache.spark.sql.Dataset

/**
  * OD数据的ID转换
  *
  * @param odLoader        od数据加载器
  * @param transformLoader 转换数据加载器
  */
class OdTransform(odLoader: OdLoader, transformLoader: TransformLoader) extends Serializable {
  /**
    * 转换Od编号
    *
    * @param odFilePath     od数据文件位置
    * @param transformTable 转换表表名
    * @return
    */
  def transform(odFilePath: String, transformTable: String = OdTransform.transformTable): Dataset[OdTransformInfo] = {
    val originOdFrame: Dataset[OdInfo] = odLoader.load(odFilePath)
    val transformFrame: Dataset[TransformInfo] = transformLoader.load(transformTable)
    OdTransform.transform(originOdFrame, transformFrame)
  }
}

object OdTransform {
  val transformTable = "station_transform"

  /**
    * 内存中的转换操作
    *
    * @param originOdFrame  od数据
    * @param transformFrame 转换表数据
    * @return
    */
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
