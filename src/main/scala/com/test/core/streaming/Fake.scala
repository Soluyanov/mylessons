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

object Fake {

  val Log = Logger.getLogger(Fake.this.getClass().getSimpleName())
case class Test(field1:String,field3:java.sql.Date)
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(
        "Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
          "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Fake")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val sc = ssc.sparkContext

     val sqlContext= new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._


    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val counts =
      KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    counts.print()
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val prefix =
      "hdfs://c6402.ambari.apache.org:8020/apps/hive/warehouse/newdata2"
    val path = new org.apache.hadoop.fs.Path(prefix)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    counts.foreachRDD(rdd => {
                              rdd.collect()
                              var utilDate = new java.util.Date()
                              var date = new java.sql.Date(utilDate.getTime())
                              val df = rdd.map( x=> Test(x,date)).toDF()
                              
                              if (hdfs.exists(path) == true) {

                              df.write.partitionBy("field3").mode(org.apache.spark.sql.SaveMode.Append).parquet("hdfs://c6402.ambari.apache.org:8020/apps/hive/warehouse/newdata2")
                              } else {

df.write.partitionBy("field3").parquet("hdfs://c6402.ambari.apache.org:8020/apps/hive/warehouse/newdata2")
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
