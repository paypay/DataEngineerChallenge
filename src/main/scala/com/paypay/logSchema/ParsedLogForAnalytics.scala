package com.paypay.logSchema

case class ParsedLogForAnalytics(
                             timestamp:String,
                             requestIp:String,
                             backendIp:String,
                             url:String,
                             epoch:Long,
                             delta:Long,
                             flag:Int,
                             sessionId:Long
                           )
