import org.apache.spark.sql.SparkSession

object AfcPairTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("AFCPair").getOrCreate()
    val trad_date = "2022-04-02"
    val extraFrame = new AfcExtract(sparkSession)
      .extraOneDay(trad_date)
    val afcPair = new AfcPair(sparkSession, extraFrame)
    val afcWriter = new AfcWriter(afcPair)
    afcWriter.writeOd()
  }
}
