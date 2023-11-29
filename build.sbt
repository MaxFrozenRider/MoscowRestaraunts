
ThisBuild / version := "LATEST"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "Otus-homework-api"
  )

val sparkVersion = "3.2.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
  "org.scalatest" %% "scalatest" % "3.2.11" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % Test
)
