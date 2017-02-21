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
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

/**
  * Application used to write rsyslog messages to hdfs
  */
object Fake {

  val Log = Logger.getLogger(Fake.this.getClass().getSimpleName())
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(
        "Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
          "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads, source, pathToStore) = args
    val sparkConf = new SparkConf().setAppName("Fake")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    
    /**
      * Writes messages in files and specifies the behavior, when target
      * file exists or not
      */
    def writeLogFiles(facilityLevel: String, msg: String) = {
      val path = new Path(pathToStore + "/" + facilityLevel)
      def defineOutputStream(fsDataOutputStream: FSDataOutputStream) = {
        val bufferedWriter = new BufferedWriter(
          new OutputStreamWriter(fsDataOutputStream))
        bufferedWriter.write(msg + "\n")
        bufferedWriter.close()
      }

      def append() = defineOutputStream(hdfs.append(path))

      def create() = defineOutputStream(hdfs.create(path))

      if (hdfs.exists(path)) append() else create()

    }

    /** Defines facility level, when Kafka is a source */
    def defineFacilityLevelKafka(message: String) = message.split("\t").head

    /** Defines facility level, when TCP socket is a source */
    def defineFacilityLevelSocket(buff: Array[Byte]) = new String(buff.slice(1,3), "UTF-8")
    
    /** Defines message, when Kafka is a source */
    def defineMessageKafka(message: String) = message.split("\t")(3)

    /** Defines message, when TCP socket is a source */
    def defineMessageSocket(buff: Array[Byte]) = {
      new String(buff.drop(4), "UTF-8")
    }

   val conf = HBaseConfiguration.create()
   val tableName = "t1"
   conf.set(TableInputFormat.INPUT_TABLE, tableName)
   val myTable = new HTable(conf, tableName)
   var startTime = System.currentTimeMillis()
   val messages =
   KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
   messages.print()
   messages.foreachRDD(rdd => {rdd.foreach(message => {
                                                                   val facilityLevel = defineFacilityLevelKafka(message)
                                                                   val msg = defineMessageKafka(message)
                                                                   writeLogFiles(facilityLevel, msg)
                                                                   var p = new Put(System.currentTimeMillis().toString.getBytes())
                                                                   p.add("cf".getBytes(), "column_name".getBytes(), new String(msg).getBytes())
                                                                   myTable.put(p)
                                                                   myTable.flushCommits()
                                                                   
                                                                                                                                     
                                                                   
                                                         }
                                             )
                                 
  
          
      })

   val words = messages.map(x => (x.split("\t")(0).split('.')(1), 1))   //.foreachRDD(rdd=>(rdd.filter(x => x(0) == "info")))
   val filtered = words.filter(x => x._1 == "info") 
   filtered.print()                  //   ((a:String,b:Int)=> a=="info")
   val windowedWordCounts = filtered.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(30))
   windowedWordCounts.print()

/**
   windowedWordCounts.foreachRDD(rdd => {rdd.collect().foreach(infoCounts:Array[String] => {
                                                   writeLogFiles("infoCounts", infoCounts._2.toString)

                                                                                        }
                                                               )
                                        }
                                )

   */
   
    
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
