package flow

import java.sql.Timestamp

case class SectionFlow(start_time: Timestamp, end_time: Timestamp, start_station_id: String, end_station_id: String, flow: Double) {

}
