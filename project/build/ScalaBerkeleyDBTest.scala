import sbt._

class ScalaBerkeleyDBTest( info: ProjectInfo ) extends DefaultProject( info ) {
   val bdbje = "com.sleepycat" % "je" % "4.1.7"
   val oracleRepo = "Oracle Repository" at "http://download.oracle.com/maven"

   val scala_tools_snapshots = ("Scala-Tools Maven2 Repository - snapshots" at
       "http://scala-tools.org/repo-snapshots")
   val scala_stm = "org.scala-tools" %% "scala-stm" % "0.3-SNAPSHOT"
}
