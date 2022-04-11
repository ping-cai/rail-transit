package crowding

import java.sql.Timestamp

case class SectionWithTimeSeq(section_id: Int, start_time: Timestamp, end_time: Timestamp,
                              start_time_seq: Int, end_time_seq: Int, flow: Double,
                              ds: java.sql.Date) {

}
