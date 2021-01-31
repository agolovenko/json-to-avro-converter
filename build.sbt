import sbt.Opts.resolver.{sonatypeSnapshots, sonatypeStaging}

lazy val scala212               = "2.12.13"
lazy val scala211               = "2.11.12"
lazy val supportedScalaVersions = Seq(scala212, scala211)

organization := "io.github.agolovenko"
homepage := Some(url("https://github.com/agolovenko/json-to-avro-converter"))
scmInfo := Some(ScmInfo(url("https://github.com/agolovenko/json-to-avro-converter"), "git@github.com:agolovenko/json-to-avro-converter.git"))
developers := List(Developer("agolovenko", "agolovenko", "ashotik@gmail.com", url("https://github.com/agolovenko")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

scalaVersion := scala212

lazy val root = project
  .in(file("."))
  .enablePlugins(GitVersioning)
  .settings(
    name := "json-to-avro-converter",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= new Dependencies(scalaVersion.value).all,
    publishMavenStyle := true,
    publishTo := Some(if (isSnapshot.value) sonatypeSnapshots else sonatypeStaging)
  )
