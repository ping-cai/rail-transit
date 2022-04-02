package od

import java.sql.Timestamp

case class OdTransformInfo(departureTime: Timestamp, arrivalTime: Timestamp,
                           departureStationId: Int, arrivalStationId: Int, passengers: Int) {

}
