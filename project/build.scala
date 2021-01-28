import sbt._

object Dependencies {
  private val apacheAvro = "org.apache.avro"   % "avro"       % "1.10.1"
  private val playJson   = "com.typesafe.play" %% "play-json" % "2.7.4"
  private val scalaTest  = "org.scalatest"     %% "scalatest" % "3.2.3"

  val all: Seq[ModuleID] = Seq(
    playJson   % Compile,
    apacheAvro % Compile,
    scalaTest  % Test
  )
}
