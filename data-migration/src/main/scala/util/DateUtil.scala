package util

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object DateUtil {
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val timeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")

  def getCurrentDateTime: String = {
    LocalDateTime.now.format(dateTimeFormatter)
  }

  def getCurrentDate: String = {
    LocalDateTime.now.format(dateFormatter)
  }

  def getTimeSeq(time: String, granularity: Int): Int = {
    val times = time.split(":")
    if (granularity == 15) {
      times(0).toInt * 4 + (times(1).toInt / 15)
    } else if (granularity == 30) {
      times(0).toInt * 2 + (times(1).toInt / 30)
    } else {
      times(0).toInt
    }
  }
}
