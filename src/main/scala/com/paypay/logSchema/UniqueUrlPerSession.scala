package com.paypay.logSchema

case class UniqueUrlPerSession(
                                sessionId:Long,
                                requestIp: String,
                                uniqueUrlPerSession:Long
                              )
