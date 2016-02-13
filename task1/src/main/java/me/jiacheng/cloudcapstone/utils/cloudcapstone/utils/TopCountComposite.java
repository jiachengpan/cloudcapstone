package me.jiacheng.cloudcapstone.utils.cloudcapstone.utils;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by jiacheng on 30/01/16.
 */
public class TopCountComposite {
    public static class Map extends Mapper<Text, Text, Text, TextArrayWritable> {
        protected static int keyLength;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            keyLength = context.getConfiguration().getInt("keyLength", 1);
        }

        public void map(Text key, Text val, Context context)
            throws IOException, InterruptedException {
            String[] keys = key.toString().split(",");
            String[] fields = new String[keys.length - keyLength + 1];

            System.arraycopy(keys, keyLength, fields, 0, keys.length - keyLength);
            fields[fields.length-1] = val.toString();

            context.write(
                    new Text(String.join(",", Arrays.copyOfRange(keys, 0, keyLength))),
                    new TextArrayWritable(fields));
        }
    }

    public static class ReduceReverse extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {
        protected static int N;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            N = context.getConfiguration().getInt("topN", 10);
        }

        public void reduce(Text key, Iterable<TextArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            TreeSet<Pair<Long, String[]>> count2key = new TreeSet<>(
                    (a, b) -> a.getFirst().compareTo(b.getFirst()));

            for (TextArrayWritable val: values) {
                String[] fields = val.toStrings();

                // the last field is the value for comparison
                // others are part of the composite key
                count2key.add(new Pair<>(Long.parseLong(fields[fields.length-1]),
                        Arrays.copyOfRange(fields, 0, fields.length-1)));

                if (count2key.size() > N) {
                    count2key.remove(count2key.last());
                }
            }

            for (Pair<Long, String[]> item: count2key) {
                List<String> valFields = new ArrayList<>(Arrays.asList(item.getSecond()));
                valFields.add(item.getFirst().toString());

                context.write(key,
                        new TextArrayWritable(valFields.toArray(new String[valFields.size()]))
                );
            }
        }
    }
}

