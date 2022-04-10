import afc.AfcReceiver
import org.apache.spark.sql.SparkSession

object AfcReceiverTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("AfcReceiverTest").getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val afcReceiver = new AfcReceiver(sparkSession)
    afcReceiver.sinkAfc()
  }
}
