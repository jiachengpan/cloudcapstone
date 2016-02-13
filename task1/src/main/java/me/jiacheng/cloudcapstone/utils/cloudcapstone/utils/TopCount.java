package me.jiacheng.cloudcapstone.utils.cloudcapstone.utils;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

/**
 * Created by jiacheng on 30/01/16.
 */
public class TopCount {
    // mapper
    public static class Map extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        protected static int N;
        protected TreeSet<Pair<Long, String>> count2key;

        public Map() throws IOException, InterruptedException {
            count2key = new TreeSet<>((a, b) -> b.getFirst().compareTo(a.getFirst()));
        }

        @Override
        protected void setup(Context context)
            throws IOException, InterruptedException {

            N = context.getConfiguration().getInt("topN", 10);
        }

        public void map(Text key, Text val, Context context)
            throws IOException, InterruptedException {
            count2key.add(new Pair<>(
                    Long.parseLong(val.toString()), key.toString()
            ));
            if (count2key.size() > N) {
                count2key.remove(count2key.last());
            }
        }

        @Override
        protected void cleanup(Context context)
            throws IOException, InterruptedException {
            for (Pair<Long, String> item: count2key) {
                String[] strings = { item.getSecond(), item.getFirst().toString()};
                context.write(NullWritable.get(), new TextArrayWritable(strings));
            }
        }
    }

    public static class Reduce extends Reducer<NullWritable, TextArrayWritable, Text, LongWritable> {
        protected static int N;
        protected TreeSet<Pair<Long, String>> count2key;

        public Reduce() throws IOException, InterruptedException {
            count2key = new TreeSet<>((a, b) -> b.getFirst().compareTo(a.getFirst()));
        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            N = context.getConfiguration().getInt("topN", 10);
        }

        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            for (TextArrayWritable val: values) {
                Text[] pair = (Text[]) val.toArray();

                count2key.add(new Pair<>(
                        Long.parseLong(pair[1].toString()), pair[0].toString()
                ));

                if (count2key.size() > N) {
                    count2key.remove(count2key.last());
                }
            }
            for (Pair<Long, String> item: count2key) {
                context.write(new Text(item.getSecond()), new LongWritable(item.getFirst()));
            }
        }
    }

    public static class MapReverse extends Map {
        public MapReverse() throws IOException, InterruptedException {
            count2key = new TreeSet<>((a, b) -> a.getFirst().compareTo(b.getFirst()));
        }
    }

    public static class ReduceReverse extends Reduce {
        public ReduceReverse() throws IOException, InterruptedException {
            count2key = new TreeSet<>((a, b) -> a.getFirst().compareTo(b.getFirst()));
        }
    }
}
