package flow

import java.sql.Timestamp

case class StationFlow(start_time: Timestamp, end_time: Timestamp, station_id: String, in_flow: Double, out_flow: Double) {

}
