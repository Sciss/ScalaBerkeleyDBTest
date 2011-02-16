import sbt._

class ScalaBerkeleyDBTest( info: ProjectInfo ) extends DefaultProject( info ) {
   val bdbje = "com.sleepycat" % "je" % "4.1.7" // from
   val oracleRepo = "Oracle Repository" at "http://download.oracle.com/maven" // maven/com/sleepycat/je/4.1.7/je-4.1.7.pom"
}
