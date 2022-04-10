package util

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object DateUtil {
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def getCurrentDateTime: String = {
    LocalDateTime.now.format(dateTimeFormatter)
  }

  def getCurrentDate: String = {
    LocalDateTime.now.format(dateFormatter)
  }
}
