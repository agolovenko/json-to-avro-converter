lazy val scala212               = "2.12.13"
lazy val scala211               = "2.11.12"
lazy val supportedScalaVersions = Seq(scala212, scala211)

ThisBuild / scalaVersion := scala212
ThisBuild / Test / fork := true

lazy val root = project
  .in(file("."))
  .enablePlugins(GitVersioning)
  .settings(
    organization := "org.echo",
    name := "json-to-avro-converter",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= new Dependencies(scalaVersion.value).all
  )
