name := "final"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,

  // new shit
  "co.theasi" %% "plotly" % "0.2.0"
  //"org.plotly-scala" %% "plotly-almond" % "0.8.0"
  //"org.plotly-scala" %% "plotly-render" % "0.8.0"

  // bokeh scala
//  "io.continuum.bokeh" % "bokeh_2.10" % "0.5",
//  "org.scalanlp" %% "breeze" % "0.5",
//  "org.scalanlp" %% "breeze-viz" % "0.5",
//  "org.scalatest" %% "scalatest" % "3.0.5",
//  "org.apache.spark" %% "spark-core" % "2.4.0",
//  "org.apache.spark" %% "spark-mllib" % "2.4.0"

)
