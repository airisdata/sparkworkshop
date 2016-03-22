package com.airisdata.referenceapp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

/**
  * Created by timothyspann on 3/21/16.
  */

case class LogRecord( clientIp: String, clientIdentity: String, dateTime: String, request:String, statusCode:String, bytesSent:String, referer:String, userAgent:String )

object Logs {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("Logs")
    sparkConf.set("spark.cores.max", "2")
    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    sparkConf.set("spark.sql.tungsten.enabled", "true")
    sparkConf.set("spark.eventLog.enabled", "true")
    sparkConf.set("spark.app.id", "Logs")
    sparkConf.set("spark.io.compression.codec", "snappy")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")

    println("# of Cores Available: ", Runtime.getRuntime().availableProcessors())

    // Get Spark Context
    val sc = new SparkContext(sparkConf)

    // Build RDD from Log File
    // Example Apache HTTP Log Line
    // 107.170.237.191 - - [20/Mar/2016:17:09:22 -0700] "GET /logs/access.log HTTP/1.1" 200 95316 "http://redlug.com/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36" "redlug.com"
    // IP Client(Space)Client(Space)User(Space)DateTime(Space)Request(Space)StatusCode(Space)Bytes(Space)Referer(Space)Agent
    // missing data is a -
    // See:   http://www.restapitutorial.com/httpstatuscodes.html
    // 404 Not Found, 400 Bad Request, 401 Unauthorized
    // 500 internal error, 501 not implemented, 502 bad gatweway

    val logFile = sc.textFile("access2.log")

    println("Log count: " +  logFile.count())

    val chunks = logFile.map {
      line =>
        val part = line.split(" ")
        val id = part(0)
        val record = LogRecord( part(0), part(1), part(2), part(3), part(4), part(5), part(6), part(7) )
        (id, record)
    }

    chunks.cache()

    try {
      chunks.take(10).foreach(println)

    } catch
    {
      case e: Exception =>
      println("Exception:" + e.getMessage);
      e.printStackTrace();
    }

    // action

//    println("Baidu Spider Access: %s", chunks.filter(_.contains("Baiduspider")).count())
//    println("Googlebot Spider Access: %s", chunks.filter(_.contains("Googlebot")).count())
//    println("404 Pages Not Found: %s", chunks.filter(_.contains("404")).count())
//    println("500 Internal Errors: %s", chunks.filter(_.contains("500")).count())

//    // Filter file by string
//    val errors = logFile.filter( _.contains("500"))
//
//    println("Errors ", errors.count() )
//    println("Errors ", errors.first())
//
//    val warnings = logFile.filter(_.contains("404"))
//
//    println("Warnings ", warnings.count())
//    println("Warnings ", warnings.first())

    sc.stop()
  }
}
