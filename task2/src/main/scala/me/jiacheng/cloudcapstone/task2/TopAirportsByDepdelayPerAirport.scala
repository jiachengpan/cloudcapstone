package me.jiacheng.cloudcapstone.task2

import java.io.StringReader

import com.datastax.spark.connector._
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jiacheng on 13/02/16.
  */

/**
  * For each origin airport X, rank the top-10 destination airports in decreasing order of on-time departure performance from X.
  */

object TopAirportsByDepdelayPerAirport {
  def mapLine2Fields(line: (String, String)) : ((String, String), Int) = {
    val parser = new CSVParser(new StringReader(line._2), CSVFormat.DEFAULT)
    val record = parser.getRecords.get(0)

    val delay = if (record.get(Setup.COL_DEPDELAYM).isEmpty) 0 else record.get(Setup.COL_DEPDELAYM).toFloat.toInt

    ((record.get(Setup.COL_ORIGIN), record.get(Setup.COL_DEST)), delay)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("usage: TopAirportsByDepdelayPerAirport <zookeeprQuorum> <group> <topics> <numThreads>")
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
      .reduceByKey(_ + _)
      .map{ case ((airport, dest), delay) => (airport, (dest, delay)) }
      .groupByKey()
      .map{ case (airport, destvals) =>
        val topCarriers = destvals.toList.sortBy(_._2).take(10)
        (airport, topCarriers.map(_._1), topCarriers.map(_._2))
      }

    airportCarrier2delay.foreachRDD(
      _.saveToCassandra("cloudcapstone", "topdestsbydepdelayperairport",
        SomeColumns("source_airport", "top_dst_airports", "top_dst_airports_depdelay")))

    ssc.start()
    ssc.awaitTermination()
  }

}
