package payPay.logSchema

case class UniqueUrlPerSession(
                                sessionId:Long,
                                requestIp: String,
                                uniqueUrlPerSession:Long
                              )
