package me.jiacheng.cloudcapstone.utils.cloudcapstone.utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jiacheng on 07/02/16.
 */
public class ReduceSum extends Reducer<Text, IntWritable, Text, LongWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
