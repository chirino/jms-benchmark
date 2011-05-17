import sbt._

class BenchmarkProject(info: ProjectInfo) extends DefaultProject(info) {

  lazy  val maven_local = "Local Maven Repository" at "file://" + Path.userHome + "/.m2/repository"

  lazy val karaf_console = "org.apache.karaf.shell" % "org.apache.karaf.shell.console" % "2.1.0"
  lazy val slf4j_nop = "org.slf4j" % "slf4j-nop" % "1.6.0"
  lazy val activemq = "org.apache.activemq" % "activemq-core" % "5.4.2"

}

