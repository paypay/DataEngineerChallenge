package com.paypay.logSchema

case class LogParser(
  timestamp:String,
  elbName:String,
  requestIp:String,
  requestPort:String,
  backendIp:String,
  backendPort:String,
  requestProcessingTime:Double,
  backendProcessingTime:Double,
  clientResponseTime:Double,
  elbResponseCode:String,
  backendResponseCode:String,
  receivedBytes:Long,
  sentBytes:Long,
  requestVerb:String,
  url:String,
  protocol:String,
  userAgent:String,
  sslCipher:String,
  sslProtocol:String)


