package com.paypay

import com.paypay.common.Spark
import com.paypay.logSchema.{CountPerSession, LongestSession, UniqueUrlPerSession}
import com.paypay.parser.{SessionLog, Sessions}
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import com.paypay.parser.SessionLog.{logMapping, logRegex}

class LogParserHelper extends AnyFlatSpec with should.Matchers with Spark {

  val sparkSession = createSession(true);
  val maxSession = 90

  import sparkSession.implicits._


  val logs = sparkSession
    .sparkContext
    .textFile("sample.txt")
    .filter(line=>line.matches(logRegex.toString))
    .map(line=>logMapping(line)).toDF()


  val expectedSessions = Seq(
    CountPerSession("103.251.186.2",0,15),
    CountPerSession("103.251.186.2",1,14),
    CountPerSession("103.251.186.2",2,14),
    CountPerSession("103.251.186.2",3,4),
    CountPerSession("103.251.186.2",4,2)
  )

  val expectedSessionsforUniqueUrlPerSession = Seq(
    UniqueUrlPerSession(1,"203.24.1.137",17),
    UniqueUrlPerSession(2,"203.24.1.137",16),
    UniqueUrlPerSession(4,"203.24.1.137",1),
    UniqueUrlPerSession(3,"203.24.1.137",6),
    UniqueUrlPerSession(0,"203.24.1.137",13)
  )

  val expectedSessionsforLongestSession= Seq(
    LongestSession("117.255.250.172",66141)
  )


  "Sessionizing user clicks" must "return sessions for single user" in {
    val sessionizedLogs = Sessions.logWithSchema(Sessions.sessionize(logs, maxSession)(sparkSession))(sparkSession)
    val sessionIp = sessionizedLogs.filter($"requestIp"==="103.251.186.2")

    val df = SessionLog.countPerSession(sessionIp)(sparkSession).orderBy(col("ipHits"), col("sessionId"))
    val expectedDf = expectedSessions.toDS().orderBy(col("ipHits"), col("sessionId"))

    assert(df.printSchema() == expectedDf.printSchema())

    assert(df.take(10).toSeq === expectedDf.take(10).toSeq)
  }


  "Unique URL" must "contains in per sessions" in {
    val sessionizedLogs = Sessions.logWithSchema(Sessions.sessionize(logs, maxSession)(sparkSession))(sparkSession)
    val sessionIp = sessionizedLogs.filter($"requestIp"==="203.24.1.137")

    val df = SessionLog.uniqueUrlPerSession(sessionIp)(sparkSession).orderBy(col("uniqueUrlPerSession"), col("sessionId"))

    val expectedDf = expectedSessionsforUniqueUrlPerSession.toDS().orderBy(col("uniqueUrlPerSession"), col("sessionId"))

    assert(df.printSchema() == expectedDf.printSchema())

    assert(df.take(10).toSeq === expectedDf.take(10).toSeq)

  }

  "Longest URL time" must "contains per sessions" in {
    val sessionizedLogs = Sessions.logWithSchema(Sessions.sessionize(logs, maxSession)(sparkSession))(sparkSession)
    val sessionIp = sessionizedLogs.filter(col("requestIp") === "117.255.250.172")

    val df = SessionLog.longestTimeIp(sessionIp)(sparkSession)

    val expectedDf = expectedSessionsforLongestSession.toDS()

    assert(df.printSchema() == expectedDf.printSchema())

    assert(df.take(10).toSeq === expectedDf.take(10).toSeq)
  }



}
