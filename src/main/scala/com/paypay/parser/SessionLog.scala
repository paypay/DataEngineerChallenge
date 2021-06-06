package com.paypay.parser

import com.paypay.common.Spark
import com.paypay.logSchema.{CountPerSession, LogParser, LongestSession, ParsedLogForAnalytics, UniqueUrlPerSession}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SessionLog extends Spark{
  
  /**
    * Pattern for parsing the Log.
    */
  val logRegex = "([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \\\"([^ ]*) ([^ ]*) (- |[^ ]*)\\\" (\"[^\"]*\") ([A-Z0-9-]+) ([A-Za-z0-9.-]*)$".r
  
  /**
    * First Requirement :
    * Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    * This method will get the count of URLs as per the User per Session.
    * @param log
    * @param sparkSession
    * @return Dataset[CountPerSession]
    */
  def countPerSession(log:Dataset[ParsedLogForAnalytics])(implicit sparkSession: SparkSession):Dataset[CountPerSession]={
    import sparkSession.implicits._
    log.groupBy("requestIp","sessionId").agg(count($"url").as("ipHits"))
      .map(row => CountPerSession(
        row.getAs[String]("requestIp"),
        row.getAs[Long]("sessionId"),
        row.getAs[Long]("ipHits")))
  }
  
  /**
    * Second Requirement :
    * Determine the average session time
    * This will get the average session time for the user and the session.
    * @param log
    * @param sparkSession
    * @return DataFrame
    */
  def averageSessionTime(log:Dataset[ParsedLogForAnalytics])(implicit sparkSession: SparkSession):DataFrame={
    import sparkSession.implicits._
    log
      .groupBy("sessionId","requestIp")
      .agg(max($"epoch").minus(min($"epoch")).as("interval"))
      .groupBy().agg(avg($"interval").as("averageSessionTime"))
  }
  
  /**
    * Third Requirement:
    * Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    * Calculate the unique url per session with respect to user.
    * @param log
    * @param sparkSession
    * @return Dataset[UniqueUrlPerSession]
    */
  def uniqueUrlPerSession(log:Dataset[ParsedLogForAnalytics])(implicit sparkSession: SparkSession):Dataset[UniqueUrlPerSession]={
    import sparkSession.implicits._
    log
      .groupBy("requestIp","sessionId")
      .agg(countDistinct($"url").as("uniqueUrlPerSession"))
      .map(row =>
        UniqueUrlPerSession(
          row.getAs[Long]("sessionId"),
          row.getAs[String]("requestIp"),
          row.getAs[Long]("uniqueUrlPerSession")))
  }
  
  /**
    * Fourth Requirement :
    * Find the most engaged users, ie the IPs with the longest session times
    * This will generate the Ips list which are mostly engaged with the activity.
    * @param log
    * @param sparkSession
    * @return Dataset[LongestSession]
    */
  def longestTimeIp(log:Dataset[ParsedLogForAnalytics])(implicit sparkSession: SparkSession):Dataset[LongestSession]={
    import sparkSession.implicits._
    log
      .groupBy("requestIp","sessionId")
      .agg(max($"epoch").minus(min($"epoch")).as("interval"))
      .groupBy("requestIp").agg(max($"interval").as("longestSessionTime"))
      .orderBy($"longestSessionTime".desc)
      .map(row=>LongestSession(row.getAs[String]("requestIp"),row.getAs[Long]("longestSessionTime")))
  }

  /**
    * This method will sessionize the WebIP.
    * @param maxSession
    * @param sparkSession
    * @return DataFrame
    */
  def parseLog(maxSession:Long,path:String)(implicit sparkSession: SparkSession):DataFrame={
    import sparkSession.implicits._
    val window = Window.partitionBy(col("requestIp")).orderBy(col("epoch").asc)
    val rawLog = sparkSession.sparkContext.textFile(path)
    val logs = rawLog.filter(line=>line.matches(logRegex.toString)).map(line=>logMapping(line)).toDF()

    val logsWithTimestamp = logs.withColumn("epoch",func_getEPoch(col("timestamp")))
    val previousTimestamp = lag($"epoch",1).over(window)
    val ts_delta = $"epoch"-previousTimestamp
    val logWithFlag = logsWithTimestamp.withColumn("delta",ts_delta).na.fill(0)
      .withColumn("flag",when(col("delta")<lit(maxSession),lit(0)).otherwise(lit(1)))
        .withColumn("sessionId",sum($"flag").over(Window.partitionBy("requestIp").orderBy("requestIp","epoch"))).cache()
    logWithFlag
  }
  
  /**
    * This method will convert the String DateTimestamp into EpochSeconds [Long]
    * @param dateString
    * @return Long
    */
  def getEpoch(dateString:String): Long ={
    val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ")
    val epoch = ZonedDateTime.parse(dateString.format(dateTimeFormat)).toEpochSecond
    epoch
  }
  
  /**
    * Create of UDF for the converting the DateTimeStamp in SparkSql.
    */
  val func_getEPoch = udf(getEpoch _)
  
  /**
    * Schema Mapping of Logs into the specified regex
    * It will return the object of the LogParser [Case Class for fields value in Logs]
    * @param line
    * @return LogParser
    */
  def logMapping(line: String):LogParser = {
    val logRegex(timestamp,elb_name,requestIp,request_port,backend_ip,backend_port,request_processing_time,backend_processing_time,client_response_time,
    elb_response_code,backend_response_code,received_bytes,sent_bytes,request_verb,url,protocol,user_agent,ssl_cipher,ssl_protocol) = line;

    LogParser(timestamp,elb_name,requestIp,request_port,backend_ip,backend_port,request_processing_time.toDouble,
      backend_processing_time.toDouble,client_response_time.toDouble,
      elb_response_code,backend_response_code,received_bytes.toLong,sent_bytes.toLong,request_verb,url,protocol,user_agent,ssl_cipher,ssl_protocol);
  }
}

