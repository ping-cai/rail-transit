package path

import config.{DistributionConf, RailTransitTable}
import domain.{Edge, Graph}
import jdbc.MysqlConf
import org.apache.spark.sql.{Encoder, Encoders}

import scala.collection.JavaConverters._

/**
  * 加载路网图
  */
class RoadNetWorkLoader(mysqlConf: MysqlConf) extends Serializable {
  private val sectionTable: String = RailTransitTable.sectionTable
  private val stationTable: String = RailTransitTable.stationTable
  private val transferPenalties: Int = DistributionConf.transferPenalties
  private val transferTimeMs: Int = DistributionConf.transferTime * 1000
  val edgeInfoEncoder: Encoder[EdgeInfo] = Encoders.kryo[EdgeInfo]
  private val edgeInfoArray: Array[EdgeInfo] = {
    val actualEdges = loadActual()
    val virtualEdges = loadVirtual()
    actualEdges ++ virtualEdges
  }
  val graph: Graph = {
    val edges = edgeInfoArray.map(x => {
      x.edge
    }).toList.asJava
    new Graph(edges)
  }
  val sectionMap: Map[Edge, Int] = {
    edgeInfoArray.filter(x => {
      x.direction != 0
    }).map(x => (x.edge, (x.edge.getWeight * 1000).toInt)).toMap
  }
  val transferMap: Map[Edge, Int] = {
    edgeInfoArray.filter(x => x.direction == 0)
      .map(x => (x.edge, transferTimeMs)).toMap
  }
  val edgeInfoMap: Map[Edge, EdgeInfo] = {
    edgeInfoArray.map(x => {
      (x.edge, x)
    }).toMap
  }

  def loadActual(): Array[EdgeInfo] = {
    val sectionFrame = mysqlConf.load(sectionTable)
    sectionFrame.createOrReplaceTempView("section_info")
    val sparkSession = sectionFrame.sparkSession
    val ratio = sparkSession.sql(
      """
SELECT SUM(travel_time)/SUM(length) ratio
FROM section_info
WHERE travel_time IS NOT NULL
      """.stripMargin
    ).first().getAs[Double]("ratio")
    sectionFrame.map(x => {
      val id = x.getAs[Int]("id")
      val station_in = x.getAs[Int]("station_in").toString
      val station_out = x.getAs[Int]("station_out").toString
      val section_direction = x.getAs[Int]("section_direction")
      val line_name = x.getAs[String]("line_name")
      val length = x.getAs[Double]("length")
      val travel_time = {
        val temp = x.getAs[Int]("travel_time")
        if (temp.isValidInt) {
          (length * ratio).intValue()
        } else {
          temp
        }
      }
      val edge = new Edge(station_in, station_out, travel_time)
      val edgeInfo = new EdgeInfo(id, edge, section_direction, line_name)
      edgeInfo
    })(edgeInfoEncoder).collect()
  }

  def loadVirtual(): Array[EdgeInfo] = {
    val stationFrame = mysqlConf.load(stationTable)
    stationFrame.createOrReplaceTempView(stationTable)
    val sparkSession = stationFrame.sparkSession
    val virtualSectionFrame = sparkSession.sql(
      s"""SELECT
	origin.id station_in,
	target.id station_out
FROM
	(
	SELECT
		id,
		name
	FROM
		$stationTable
	WHERE
	name IN ( SELECT name FROM $stationTable GROUP BY name HAVING count(*)>=2)) origin
	JOIN $stationTable target ON origin.name = target.name
WHERE
	origin.id <> target.id
GROUP BY
	origin.id,
	target.id
ORDER BY
	origin.id""")
    val virtualTable = "virtual_section"
    virtualSectionFrame.createOrReplaceTempView(virtualTable)
    val sectionWithLine = sparkSession.sql(
      s"""SELECT station_in,station_out,target1.line_name out_line,target2.line_name in_line
from $virtualTable origin
join $stationTable target1 on station_in=target1.id
join $stationTable target2 on station_out=target2.id""".stripMargin)

    sectionWithLine.map(x => {
      val station_in = x.getAs[Int]("station_in").toString
      val station_out = x.getAs[Int]("station_out").toString
      val out_line = x.getAs[String]("out_line")
      val in_line = x.getAs[String]("in_line")
      val edge = new Edge(station_in, station_out, transferPenalties)
      val edgeInfo = new EdgeInfo(0, edge, 0, s"$out_line $in_line")
      edgeInfo
    })(edgeInfoEncoder).collect()
  }

}
