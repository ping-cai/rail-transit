package section

case class SectionInfo(id: Int, station_in: Int, station_out: Int,
                       section_direction: Int, line_name: String, length: Double,
                       travel_time: Int = 0) {

}
