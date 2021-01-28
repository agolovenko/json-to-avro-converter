scalaVersion in ThisBuild := "2.11.12" // also works with "2.12.12"
organization in ThisBuild := "agolovenko"

fork in Test in ThisBuild := true

lazy val root = project
  .in(file("."))
  .enablePlugins(GitVersioning)
  .settings(
    name := "json-to-avro-converter",
    libraryDependencies ++= Dependencies.all
  )
