package me.jiacheng.cloudcapstone.task2

import java.io.StringReader

import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime

/**
  * Created by jiacheng on 13/02/16.
  */
/**
  * For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.
  */

object SuitableRoutes {
  object JourneyType extends Enumeration {
    type JourneyType = Value
    val FirstLeg, SecondLeg = Value
  }
  import JourneyType._

  // orig, dest, carrier, depart-delay, depart-time, date
  def mapLine2Fields(line: (String, String)) : (String, String, String, Int, Int, DateTime) = {
    val parser = new CSVParser(new StringReader(line._2), CSVFormat.DEFAULT)
    val record = parser.getRecords.get(0)

    if (record.get(Setup.COL_DEPDELAYM).isEmpty ||
      record.get(Setup.COL_DEPTIME).isEmpty ||
      record.get(Setup.COL_YEAR).isEmpty ||
      record.get(Setup.COL_MONTH).isEmpty ||
      record.get(Setup.COL_DAYOFMONTH).isEmpty ) {
      return null
    }

    val depdate = new DateTime(
      record.get(Setup.COL_YEAR).toInt, record.get(Setup.COL_MONTH).toInt, record.get(Setup.COL_DAYOFMONTH).toInt,
      0, 0, 0)
    (
      record.get(Setup.COL_ORIGIN),
      record.get(Setup.COL_DEST),
      record.get(Setup.COL_CARRIER),
      record.get(Setup.COL_DEPDELAYM).toFloat.toInt,
      record.get(Setup.COL_DEPTIME).toInt,
      depdate
    )
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("usage: TopCarriersByDepdelayPerRoute <zookeeprQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName(TopCarriersByDepdelayPerAirport.toString)
      .setMaster("local[2]")
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val topicMap = topics.split(',').map((_, numThreads.toInt)).toMap
    val airportCarrier2delay = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
      .map(mapLine2Fields)
      .filter(_ != null)
      .map{ case (src, dst, carrier, depdelay, deptime, date) =>
        if (deptime < 1200) dst -> (src, dst, carrier, depdelay, deptime, FirstLeg, date)
        else src -> (src, dst, carrier, depdelay, deptime, SecondLeg, date.minusDays(2))
      } // middle point -> all data
      .groupByKey()
      .flatMap{ case (mid, data) =>
        for (a <- data.filter(_._6 == FirstLeg); b <- data.filter(_._6 == SecondLeg))
          yield (a._1, mid, b._2, a._7) -> (a._4 + b._4, a._5, b._6)
      }
      .groupByKey()
      .map{ case (k, v) => (k, v.toList.sortBy(_._1)) }


    airportCarrier2delay.print(40)
    //airportCarrier2delay.foreachRDD(
    //  _.saveToCassandra("cloudcapstone", "topcarriersbydepdelayperairport",
    //    SomeColumns("source", "destination", "top_carriers", "top_carriers_depdelay")))

    ssc.start()
    ssc.awaitTermination()
  }

}
