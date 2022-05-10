package od

import java.sql.Timestamp

case class OdInfo(start: Timestamp, end: Timestamp, in_station_id: Int, out_station_id: Int, passengers: Int) {

}
