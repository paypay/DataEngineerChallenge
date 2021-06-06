package payPay.parser

import SessionLog.{func_getEPoch, logMapping, logRegex}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import payPay.logSchema.ParsedLogForAnalytics

object Sessions {
  /**
    * This method will parse the log and transform it into the required format for analytics.
    * creating EpochSeconds on the basis of timestamp provided.
    * On that creating the delta for the Ip
    * Keeping a flag for the inactivity if the user.
    * creating session with the help of inactivity of the user.
    * @param path
    * @param maxSession
    * @param sparkSession
    * @return Dataset[ParsedLogForAnalytics]
    */
  def parseLog(path:String,maxSession:Long)(implicit sparkSession: SparkSession):Dataset[ParsedLogForAnalytics]={
    import sparkSession.implicits._

    val rawLog = sparkSession.sparkContext.textFile(path)
    val logs = rawLog.filter(line=>line.matches(logRegex.toString)).map(line=>logMapping(line)).toDF()

    logWithSchema(sessionize(logs, maxSession))
  }

  /**
   * Sessionize the log
   * @param logs
   * @param maxSession
   * @param sparkSession
   * @return DataFrame
   */
  def sessionize(logs: DataFrame, maxSession: Long)(implicit sparkSession: SparkSession): DataFrame={

    val window = Window.partitionBy(col("requestIp"),col("backendIp")).orderBy(col("epoch").asc)

    val previousTimestamp = lag(col("epoch"),1).over(window)
    val ts_delta = col("epoch")-previousTimestamp

    val sessionizedLog = logs
      .withColumn("epoch", func_getEPoch(col("timestamp")))
      .withColumn("delta",ts_delta).na.fill(0)
      .withColumn("flag",when(col("delta")<lit(maxSession),lit(0)).otherwise(lit(1)))
      .withColumn("sessionId",
        sum(col("flag"))
          .over(Window.partitionBy(
            col("requestIp"),
            col("backendIp")).orderBy("requestIp","backendIp","epoch")))

    sessionizedLog

  }

  /**
   * Parse the sessionized log and get the schema from Case Class.
   * @param sessionizedLog
   * @param sparkSession
   * @return Dataset[ParsedLogForAnalytics]
   */
  def logWithSchema(sessionizedLog: DataFrame)(implicit sparkSession: SparkSession):Dataset[ParsedLogForAnalytics] = {
    import sparkSession.implicits._
    sessionizedLog
      .select("timestamp","requestIp", "backendIp", "url", "epoch", "delta", "flag", "sessionId")
      .map(row =>
        ParsedLogForAnalytics(
          row.getAs[String]("timestamp"),
          row.getAs[String]("requestIp"),
          row.getAs[String]("backendIp"),
          row.getAs[String]("url"),
          row.getAs[Long]("epoch"),
          row.getAs[Long]("delta"),
          row.getAs[Int]("flag"),
          row.getAs[Long]("sessionId")))
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  }
}

