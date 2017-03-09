name := "skeleton"

version := "1.0"

scalaVersion := "2.10.3"


libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"

libraryDependencies += "com.github.nscala-time" % "nscala-time_2.10" % "2.0.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.28"

libraryDependencies += "org.scalaz" % "scalaz-core_2.10" % "7.1.3"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.4"

libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.10" % "1.4.1_0.1.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5"

libraryDependencies += "org.scalamock" % "scalamock-scalatest-support_2.10" % "3.2"

libraryDependencies += "org.scalanlp" % "breeze_2.10" % "0.9"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.10.4"

libraryDependencies += "com.twitter" % "parquet-hadoop" % "1.6.0"

libraryDependencies += "com.twitter" % "parquet-hive-bundle" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.2"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.3.0"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.3.0"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.4"

