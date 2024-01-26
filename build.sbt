ThisBuild / version := "1.0.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "DataExplorationAndAnalysis"
  )


val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.scalanlp" %% "breeze-viz" % "1.0"
)

libraryDependencies += "org.jfree" % "jfreechart" % "1.5.3"
libraryDependencies += "org.jfree" % "jcommon" % "1.0.23"






