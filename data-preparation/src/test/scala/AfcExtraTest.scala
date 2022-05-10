import AfcExtract.save
import org.apache.spark.sql.SparkSession

object AfcExtraTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("AFCExtract").getOrCreate()
    val relativePath = "/dwd/rail_transit/afc_record"
    val hdfsPath = s"/ods/rail_transit/afc_record"
    val aFCExtract = new AfcExtract(sparkSession, hdfsPath)
    val trad_date = "2022-04-02"
    val extraFrame = aFCExtract.extraOneDay(trad_date)
    save(extraFrame, relativePath)
  }
}
