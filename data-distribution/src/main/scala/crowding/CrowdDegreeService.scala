package crowding

import java.sql.{Date, Timestamp}

import config.DistributionConf
import flow.{FlowMigration, SectionFlow, SectionWithCrowd}
import jdbc.JDBCUtils
import org.apache.spark.sql.{Dataset, Row}
import section.{SectionInfo, SectionLoader}
import train.{TrainLoader, TrainOperationInfo, TrainOperationLoader}
import util.DateUtil

class CrowdDegreeService(val sectionLoader: SectionLoader,
                         trainLoader: TrainLoader,
                         trainOperationLoader: TrainOperationLoader) {
  def setCrowdDegree(sectionFlow: Dataset[SectionFlow]): Unit = {
    //    sectionFlow.show(false)
    val sectionInfo: Dataset[SectionInfo] = sectionLoader.defaultLoad()
    sectionFlow.createOrReplaceTempView("section_flow")
    sectionInfo.createOrReplaceTempView("section_info")
    val sparkSession = sectionFlow.sparkSession
    import sparkSession.implicits._
    val sectionFrame = sparkSession.sql(
      """
SELECT section_info.id section_id,section_flow.start_time,
section_flow.end_time,section_flow.flow,section_flow.granularity
FROM section_flow join section_info ON section_info.station_in = section_flow.start_station_id
AND section_info.station_out = section_flow.end_station_id
      """.stripMargin)
    val trainOperationInfo: Dataset[TrainOperationInfo] = trainOperationLoader.load()
    val sectionWithTimeSeq = sectionFrame.mapPartitions((partition: Iterator[Row]) => {
      partition.map(x => {
        val section_id = x.getAs[Int]("section_id")
        val start_time = x.getAs[Timestamp]("start_time")
        val end_time = x.getAs[Timestamp]("end_time")
        val flow = x.getAs[Double]("flow")
        val granularity = x.getAs[Int]("granularity")
        val start_time_seq = DateUtil.getTimeSeq(start_time.toLocalDateTime.format(DateUtil.timeFormatter), granularity)
        val end_time_seq = DateUtil.getTimeSeq(end_time.toLocalDateTime.format(DateUtil.timeFormatter), granularity)
        SectionWithTimeSeq(section_id, start_time, end_time, start_time_seq, end_time_seq, flow, granularity)
      })
    })
    //    sectionWithTimeSeq.show(false)
    sectionWithTimeSeq.createOrReplaceTempView("section_flow")
    val trainOperationWithTimeSeq: Dataset[TrainOperationWithTimeSeq] = trainOperationInfo.mapPartitions((partition: Iterator[TrainOperationInfo]) => {
      partition.flatMap(x => {
        val section_id = x.section_id
        val departure_time = x.start_time
        List(15, 30, 60).map(granularity => {
          val start_time_seq = DateUtil.getTimeSeq(departure_time, granularity)
          TrainOperationWithTimeSeq(section_id, start_time_seq, x.train_id, granularity)
        })
      })
    })
    trainOperationWithTimeSeq
      .createOrReplaceTempView("train_operation_info")
    val trainInfo = trainLoader.load(TrainLoader.trainTable)
    trainInfo.createOrReplaceTempView("train_info")
    sparkSession.sql(
      """
        |SELECT section_flow.section_id,start_time,end_time,flow,train_id,section_flow.granularity
        |FROM section_flow
        |JOIN train_operation_info
        |ON section_flow.section_id = train_operation_info.section_id
        |AND section_flow.granularity=train_operation_info.granularity
        |AND section_flow.start_time_seq<=train_operation_info.start_time_seq
        |AND section_flow.end_time_seq>train_operation_info.start_time_seq
      """.stripMargin).createOrReplaceTempView("section_flow")

    val sectionTrainFrame = sparkSession.sql(
      """
SELECT section_id,start_time,end_time,flow,granularity,sum(transport_capacity) transport_capacity,sum(staffing) staffing
FROM section_flow JOIN train_info ON section_flow.train_id = train_info.id
GROUP BY section_id,start_time,end_time,flow,granularity
      """.stripMargin)
    sectionTrainFrame
      .mapPartitions((partition: Iterator[Row]) => {
        partition.map(x => {
          val section_id = x.getAs[Int]("section_id")
          val start_time = x.getAs[Timestamp]("start_time")
          val end_time = x.getAs[Timestamp]("end_time")
          val flow = x.getAs[Double]("flow")
          val transport_capacity = x.getAs[Long]("transport_capacity").toInt
          val staffing = x.getAs[Long]("staffing").toInt
          val granularity = x.getAs[Int]("granularity")
          val crowdDegree = CrowdDegreeService.trainCostCompute(flow, transport_capacity, staffing)
          SectionWithCrowd(section_id, start_time, end_time, flow, crowdDegree, granularity)
        })
      })
      .as[SectionWithCrowd]
      .foreachPartition((partition: Iterator[SectionWithCrowd]) => {
        val connection = JDBCUtils.getConnection
        connection.setAutoCommit(false)
        val preparedStatement = connection.prepareStatement(FlowMigration.sectionSql)
        partition.foreach(x => {
          preparedStatement.setInt(1, x.section_id)
          preparedStatement.setTimestamp(2, x.start_time)
          preparedStatement.setTimestamp(3, x.end_time)
          preparedStatement.setDouble(4, x.flow)
          preparedStatement.setDate(5, new Date(x.start_time.getTime))
          preparedStatement.setDouble(6, x.crowdDegree)
          preparedStatement.setInt(7, x.granularity)
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        connection.commit()
        JDBCUtils.closeConnection(connection, preparedStatement)
      })
  }
}

object CrowdDegreeService {
  private val alpha: Double = DistributionConf.alpha
  private val beta: Double = DistributionConf.beta

  def trainCostCompute(flow: Double, transport_capacity: Int, staffing: Int): Double = {
    val seats = staffing
    val maxCapacity = (transport_capacity / 60) * 3
    var result = 0.0
    if (flow < seats) {
      result = 0
    } else if (flow <= maxCapacity) {
      result = (alpha * (flow - seats)) / seats
    } else if (flow > maxCapacity) {
      result = (alpha * (flow - seats)) / seats + (beta * (flow - maxCapacity)) / maxCapacity
    }
    result
  }
}
