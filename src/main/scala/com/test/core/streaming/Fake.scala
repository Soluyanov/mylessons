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
object Fake {

  val Log = Logger.getLogger(Fake.this.getClass().getSimpleName())
  case class streamData(Data:String,Count1:Int,Date_and_time:java.sql.Timestamp,Count2:Int, Date:java.sql.Date )
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println(
        "Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
          "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads, prefix) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Fake")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sc.getConf.getAll.foreach(println)
    println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
  

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val counts =
      KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    counts.print()
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val path = new org.apache.hadoop.fs.Path(prefix)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    hiveContext.sql("CREATE TABLE IF NOT EXISTS parquet_test10 (field1 string,field2 int,field3 timestamp,field4 int)STORED AS PARQUET")
    counts.foreachRDD(rdd => {
      var utilDate = new java.util.Date()
      var date = new java.sql.Date(utilDate.getTime())
      var ts = java.sql.Timestamp.from(java.time.Instant.now)
      val df = rdd.map(x => streamData(x, 10, ts, 20, date)).toDF()
      df.show()
      if (hdfs.exists(path) == true) {

        df.write
          .partitionBy("Date")
          .mode(org.apache.spark.sql.SaveMode.Append)
          .format("parquet")
          .save(prefix)
              } else {

        df.write.partitionBy("Date").format("parquet").save(prefix)
             }

var cs = hdfs.getContentSummary(path)
var fileCount = cs.getFileCount()
println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
println(fileCount)
println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
      if (fileCount >= 20) {
        val bufferDF = sqlContext.read.parquet(prefix)
        bufferDF
          .write
          .partitionBy("Date")
          .mode(org.apache.spark.sql.SaveMode.Overwrite)
          .format("parquet")
          .save("hdfs://c6402.ambari.apache.org:8020/apps/hive/warehouse/aggregation1")
        hdfs.delete(path)
      }

    })

    Log.error("DEBUG info:" + zkQuorum)

    sys.ShutdownHookThread({
      println("Ctrl+C")
      try {
        hdfs.close()
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

