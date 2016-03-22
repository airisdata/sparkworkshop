package com.airisdata.referenceapp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.{KryoSerializer}

/**
  * Created by timothyspann on 3/21/16.
  */

case class LogRecord( clientIp: String, clientIdentity: String, user: String, dateTime: String, request:String,
                      statusCode:Int, bytesSent:Long, referer:String, userAgent:String )

object Logs {

    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".r

    def parseLogLine(log: String): LogRecord = {
      try {
        val res = PATTERN.findFirstMatchIn(log)

        if (res.isEmpty || res.toString.equals("None")) {
          println("Rejected Log Line: " + log)
          LogRecord("Empty", "-", "-", "", "",  -1, -1, "-", "-" )
        }
        else {
          val m = res.get
          // NOTE:   Head does not have a content size.
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
          println("Exception on line:" + log + ":" + e.getMessage);
          LogRecord("Empty", "-", "-", "", "-", -1, -1, "-", "-" )
      }
    }


  //// Main Spark Program
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Logs")

    sparkConf.set("spark.cores.max", "4")
    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    sparkConf.set("spark.sql.tungsten.enabled", "true")
    sparkConf.set("spark.eventLog.enabled", "true")
    sparkConf.set("spark.app.id", "Logs")
    sparkConf.set("spark.io.compression.codec", "snappy")
    sparkConf.set("spark.rdd.compress", "true")

    val sc = new SparkContext(sparkConf)

    val logFile = sc.textFile("access2.log")

    val accessLogs = logFile.map(parseLogLine).cache()

    try {
      println("Log Count: %s".format(accessLogs.count()))
      accessLogs.take(25).foreach(println)

      // Calculate statistics based on the content size.
      val contentSizes = accessLogs.map(log => log.bytesSent).cache()
      val contentTotal = contentSizes.reduce(_ + _)

      println("Number of Log Records: %s  Content Size Total: %s, Avg: %s, Min: %s, Max: %s".format(
        contentSizes.count,
        contentTotal,
        contentTotal / contentSizes.count,
        contentSizes.min,
        contentSizes.max))
    } catch
    {
      case e: Exception =>
      println("Exception:" + e.getMessage);
      e.printStackTrace();
    }

    sc.stop()
  }
}
