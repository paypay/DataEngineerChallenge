package payPay.common

import org.apache.spark.sql.SparkSession

trait Spark {
  
  def createSession(local:Boolean):SparkSession={
    val builder = SparkSession.builder().appName(this.getClass.getSimpleName)
    if(local){
      val session = builder.master("local[*]").getOrCreate()
      session
    }else builder.getOrCreate()
  }
}
