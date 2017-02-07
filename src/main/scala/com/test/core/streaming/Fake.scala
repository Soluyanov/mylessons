package com.test.core.streaming

import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.log4j.Logger
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream._
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.io.{
  ArrayWritable,
  BooleanWritable,
  BytesWritable,
  DoubleWritable,
  FloatWritable,
  IntWritable,
  LongWritable,
  NullWritable,
  Text,
  Writable
}
import org.apache.spark.rdd.PairRDDFunctions
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.io.compress.GzipCodec
import java.io.PrintWriter
import org.apache.hadoop.fs.FSDataOutputStream
import java.io._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import scala.io.Source
import org.apache.spark.sql.functions.unix_timestamp
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import org.apache.hadoop.fs.Path
object Fake {

  val Log = Logger.getLogger(Fake.this.getClass().getSimpleName())
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println(
        "Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
          "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads, source, pathToStore) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Fake")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    def writeLogFiles(facilityLevel: String, msg: String) = {
      val path = new Path(pathToStore + "/" + facilityLevel)
      def defineOutputStream(fsDataOutputStream: FSDataOutputStream) = {
        val bufferedWriter = new BufferedWriter(
          new OutputStreamWriter(fsDataOutputStream))
        bufferedWriter
          .write(msg + "\n")
        bufferedWriter.close()
      }
      def append() = defineOutputStream(hdfs.append(path))
      def create() = defineOutputStream(hdfs.create(path))
      if (hdfs.exists(path)) append() else create()

    }

    def defineFacilityLevel(message: String) = message.split("\t").head
    def defineMessage(message: String) = {
      val fields = message.split("\t")
      fields(3)
    }
    if (source == "kafka") {

      val messages =
        KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
      messages.print()
      messages.foreachRDD(rdd => {
        rdd
          .collect()
          .foreach(
            message => {
              val facilityLevel = defineFacilityLevel(message)
              val msg = defineMessage(message)
              val prefix = pathToStore + "/" + facilityLevel
              writeLogFiles(facilityLevel, msg)

            }
          )
      })

    } else {

      val server = new ServerSocket(514)
      println("listening for connection on port ...")

      while (true) {
        val clientSocket = server.accept()
        val reader = new BufferedReader(
          new InputStreamReader(clientSocket.getInputStream()))
        var line = reader.readLine()
        while (!line.isEmpty()) {
          println(line)

          val pri =
            line.substring(line.indexOf("<") + 1, line.indexOf(">")).toInt
          val msg = line.substring(line.indexOf(": ") + 1)
          val facility = pri / 8
          val level = pri - (facility * 8)
          val facilityLevel = facility.toString + '_' + level.toString
          val prefix = pathToStore + "/" + facilityLevel
          writeLogFiles(facilityLevel, msg)
          line = reader.readLine()
        }
      }

    }

    Log.error("DEBUG info:" + zkQuorum)

    sys.ShutdownHookThread({
      println("Ctrl+C")
      try {

        ssc.stop(stopSparkContext = true, stopGracefully = true)
      } catch {
        case e: Throwable => {
          println("exception on ssc.stop(true, true) occured")
        }
      }
    })

    ssc.start()

    ssc.awaitTermination()

  }
}
