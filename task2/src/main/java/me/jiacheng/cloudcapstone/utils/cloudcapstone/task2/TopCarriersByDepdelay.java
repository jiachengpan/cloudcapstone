package me.jiacheng.cloudcapstone.utils.cloudcapstone.task2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jiacheng on 12/02/16.
 */
public class TopCarriersByDepdelay {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("TopCarriersByDepdelay");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("ttt", 1);

        JavaPairReceiverInputDStream<String, String> inputLines =
                KafkaUtils.createStream(jssc, "192.168.1.113:2181", "consumer-group", topicMap);

        JavaDStream<String> lines = inputLines.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                System.out.println("put" + tuple2._2());
                return tuple2._2();
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
