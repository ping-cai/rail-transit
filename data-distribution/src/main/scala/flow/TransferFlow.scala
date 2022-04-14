package flow

import java.sql.Timestamp

case class TransferFlow(start_time: Timestamp, end_time: Timestamp,
                        transfer_out_station_id: String, transfer_in_station_id: String,
                        transfer_out_line: String, transfer_in_line: String,
                        flow: Double, granularity: Int = 0) {

}
