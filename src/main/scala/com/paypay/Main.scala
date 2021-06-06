package com.paypay

import com.paypay.common.Spark
import com.paypay.parser.{SessionLog, Sessions}
import org.apache.spark.sql.SparkSession

object Main extends Spark {
  def main(args: Array[String]): Unit = {
    if (args.length < 2 || args.length > 3) {
      println(s"Usage: Please provide <input path> <output path> ")
      System.exit(-1)
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val localSpark = args.length == 3 && args(2) == "local"
    val maxSession = 900
    val partition = 1
    
    implicit val spark: SparkSession = createSession(localSpark)
    
    val rawLog = Sessions.parseLog(inputPath, maxSession)

    // Q1 : Sessionize = aggregrate all page hits by visitor/IP during a session.
    val countIpPerSession = SessionLog.countPerSession(rawLog)

    // Q2 : Determine the average session time
    val averageIpSessionTime = SessionLog.averageSessionTime(rawLog)

    // Q3 : count a hit to a unique URL only once per session.
    val distinctUrl = SessionLog.uniqueUrlPerSession(rawLog)

    // Q4 : Find the most engaged users, ie the IPs with the longest session times
    val longestSessionTime = SessionLog.longestTimeIp(rawLog)
  
    /**
      * write the metric in parquet file.
      */
    countIpPerSession.repartition(partition).write.parquet(outputPath+"/countIpPerSession")
    averageIpSessionTime.repartition(partition).write.parquet(outputPath+"/averageIpSessionTime")
    distinctUrl.repartition(partition).write.parquet(outputPath+"/distinctUrl")
    longestSessionTime.repartition(partition).write.parquet(outputPath+"/longestSessionTime")
  }
}
