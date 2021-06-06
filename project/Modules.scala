import Versions._
import sbt._


object Modules {

  val sparkCoreLib = "org.apache.spark" % s"spark-core_$scalaMajorVersion" % s"$sparkVersion" % "provided"

  val sparkSqlLib = "org.apache.spark" %% "spark-sql" % s"$sparkVersion" % "provided"

  val scalaTestLib = "org.scalatest" %% "scalatest" % s"$scalaTest" % "test"

  val scalatic = "org.scalactic" %% "scalactic" % s"$scalaTest"

}


