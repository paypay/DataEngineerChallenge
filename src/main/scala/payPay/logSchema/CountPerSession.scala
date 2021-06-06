package payPay.logSchema

case class CountPerSession(
                          requestIp:String,
                          sessionId:Long,
                          ipHits:Long
                          )
