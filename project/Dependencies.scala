import Modules.{scalaTestLib, sparkCoreLib, sparkSqlLib,_}
import sbt.librarymanagement.ModuleID

object Dependencies {

  val sparkDependencies: Seq[ModuleID] = Seq(sparkCoreLib, sparkSqlLib)

  val testDependencies: Seq[ModuleID] = Seq(scalaTestLib)

}
