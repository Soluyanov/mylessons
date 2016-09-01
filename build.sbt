name := "skeleton"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"

mainClass in (Compile, packageBin) := Some("com.test.core.streaming.Fake")

mainClass in (Compile, run) := Some("com.test.core.streaming.Fake")
