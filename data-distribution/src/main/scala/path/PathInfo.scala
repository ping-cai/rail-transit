package path

import java.sql.Timestamp

import domain.Path

case class PathInfo(departureTime: Timestamp, arrivalTime: Timestamp,
                    pathList: java.util.List[Path], passengers: Int) {

}
