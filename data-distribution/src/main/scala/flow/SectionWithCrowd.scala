package flow

import java.sql.Timestamp

case class SectionWithCrowd(section_id: Int, start_time: Timestamp, end_time: Timestamp, flow: Double,
                            crowdDegree: Double, granularity: Int) {

}
