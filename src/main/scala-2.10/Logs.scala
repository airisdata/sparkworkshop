package com.airisdata.referenceapp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

/**
  * Created by timothyspann on 3/21/16.
  */

case class LogRecord( clientIp: String, clientIdentity: String, user: String, dateTime: String, request:String,
                      statusCode:Int, bytesSent:Long, referer:String, userAgent:String )

object Logs {

  //// Head doesn't send bytes
  //// 192.0.101.226 - - [24/Feb/2016:06:27:43 -0500] "HEAD / HTTP/1.1" 200 - "-" "jetmon/1.0 (Jetpack Site Uptime Monitor by WordPress.com)"
  //// 157.55.39.97 - - [24/Feb/2016:04:24:18 -0500] "GET /2013/07/23/ipv4stack-for-java/ HTTP/1.1" 200 21314 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"
  //// regular expression pattern for log records
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".r

  //// parse the Common Apache Format into a LogRecord case class
    def parseLogLine(log: String): LogRecord = {
      try {
        val res = PATTERN.findFirstMatchIn(log)

        if (res.isEmpty || res.toString.equals("None")) {
          println("Log Line: " + log)

          LogRecord("Empty", "-", "-", "", "",  -1, -1, "-", "-" )
        }
        else {
          val m = res.get
          if (m.group(9).equals("-")) {
            LogRecord(m.group(1), m.group(2), m.group(3), m.group(4),
              m.group(5), m.group(8).toInt, 0, m.group(10), m.group(11))
          }
          else {
            LogRecord(m.group(1), m.group(2), m.group(3), m.group(4),
              m.group(5), m.group(8).toInt, m.group(9).toLong, m.group(10), m.group(11))
          }
        }
      } catch
      {
        case e: Exception =>
          println("Exception:" + e.getMessage);
          LogRecord("Empty", "-", "-", "", "-", -1, -1, "-", "-" )
      }
    }


  //// Main Spark Program
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("Logs")
    sparkConf.set("spark.cores.max", "4")
    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    sparkConf.set("spark.sql.tungsten.enabled", "true")
    sparkConf.set("spark.eventLog.enabled", "true")
    sparkConf.set("spark.app.id", "Logs")
    sparkConf.set("spark.io.compression.codec", "snappy")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")

    println("# of Cores Available: " + Runtime.getRuntime().availableProcessors())

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

    val accessLogs = logFile.map(parseLogLine).cache()

    try {
      println("Log Count: " + accessLogs.count())
      println("First Ten Records")
      accessLogs.take(10).foreach(println)

      // Calculate statistics based on the content size.
      val contentSizes = accessLogs.map(log => log.bytesSent).cache()
      println("Content Size Avg: %s, Min: %s, Max: %s".format(
        contentSizes.reduce(_ + _) / contentSizes.count,
        contentSizes.min,
        contentSizes.max))

      // Compute Response Code to Count.
      val responseCodeToCount = accessLogs
        .map(log => (log.statusCode, 1))
        .reduceByKey(_ + _)
        .take(100)
      println(s"""Status Code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

      // Any IPAddress that has accessed the server more than 10 times.
      val ipAddresses = accessLogs
        .map(log => (log.clientIp, 1))
        .reduceByKey(_ + _)
        .filter(_._2 > 10)
        .map(_._1)
        .take(100)
      println(s"""IP Addresses Accessed > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

    } catch
    {
      case e: Exception =>
      println("Exception:" + e.getMessage);
      e.printStackTrace();
    }

    // action

//    println("Baidu Spider Access: %s", accessLogs.filter(_.contains("Baiduspider")).count())
//    println("Googlebot Spider Access: %s", accessLogs.filter(_.contains("Googlebot")).count())
//    println("404 Pages Not Found: %s", accessLogs.filter(_.contains("404")).count())
//    println("500 Internal Errors: %s", accessLogs.filter(_.contains("500")).count())

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
