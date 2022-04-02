package section

case class TimeConverter(section: Section, seconds: Int) {

}

object TimeConverter {
  /**
    * 转换时刻表的数据
    *
    * @param departureTime 出发时间
    * @param arrivalTime   到达时间
    * @return 区间相隔分钟数
    */
  def convertSecondsTime(departureTime: String, arrivalTime: String): Int = {
    val departureArray = departureTime.split("\\.")
    val arrivalArray = arrivalTime.split("\\.")
    val departHours = departureArray(0).toInt
    val departMinutes = departureArray(1).toInt
    val departSeconds = departureArray(2).toInt
    val arrivalHours = arrivalArray(0).toInt
    val arrivalMinutes = arrivalArray(1).toInt
    val arrivalSeconds = arrivalArray(2).toInt
    val resultSeconds = {
      (arrivalHours - departHours) * 3600 + (arrivalMinutes - departMinutes) * 60 + (arrivalSeconds - departSeconds)
    }
    if (resultSeconds >= 0) {
      resultSeconds
    } else {
      resultSeconds + (24 * 3600)
    }
  }
}
