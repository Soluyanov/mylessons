name := "KafkaWord"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.0.0",
	"org.apache.spark" %% "spark-streaming" % "2.0.0",
	"org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0"
)
