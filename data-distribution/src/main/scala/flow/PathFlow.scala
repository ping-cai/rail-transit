package flow

import java.sql.Timestamp

import domain.Path

case class PathFlow(departureTime: Timestamp, arrivalTime: Timestamp, path: Path, passengers: Double) {

}
