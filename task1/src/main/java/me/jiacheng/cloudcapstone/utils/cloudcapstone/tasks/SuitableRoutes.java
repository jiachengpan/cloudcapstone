package me.jiacheng.cloudcapstone.utils.cloudcapstone.tasks;

/**
 * Created by jiacheng on 30/01/16.
 */

import com.google.common.collect.Lists;
import me.jiacheng.cloudcapstone.utils.cloudcapstone.utils.TextArrayWritable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

/**
 * Group 3 (Answer both using only Hadoop and Spark):
 * Does the popularity distribution of airports follow a Zipf distribution? If not, what distribution does it follow?
 * Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way. More concretely, Tom has the following requirements (see Task 1 Queries for specific queries):
 * The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y). For example, if X-Y departs January 5, 2008, Y-Z must depart January 7, 2008.
 * Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
 * Tom wants to arrive at each destination with as little delay as possible (Clarification 1/24/16: assume you know the actual delay of each flight).
 * Your mission (should you choose to accept it!) is to find, for each X-Y-Z and day/month (dd/mm) combination in the year 2008, the two flights (X-Y and Y-Z) that satisfy constraints (a) and (b) and have the best individual performance with respect to constraint (c), if such flights exist.
 */

public class SuitableRoutes extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SuitableRoutes(), args);
        System.exit(res);
    }

    public enum JourneyType {
        FIRST_LEG, SECOND_LEG
    }

    public static class Map extends Mapper<LongWritable, Text, Text, TextArrayWritable> {
        private static final int[] columns = {
                Setup.COL_ORIGIN, Setup.COL_DEST, Setup.COL_DEPTIME,
                Setup.COL_YEAR, Setup.COL_MONTH, Setup.COL_DAYOFMONTH,
                Setup.COL_ARRDELAYM
        };

        // compute date -> (journey_type, orig, dest, deptime, arrdly)
        public ArrayList<Pair<String, String[]>> computeKeyValues(String[] fields)
                throws IOException, InterruptedException {
            ArrayList<Pair<String, String[]>> result = new ArrayList<>();

            if (fields[2].isEmpty()) return result;
            Integer deptime = Integer.parseInt(fields[2]);

            List<String> valArr = new ArrayList<>();

            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
            Calendar date = Calendar.getInstance();
            date.set(Integer.parseInt(fields[3]), Integer.parseInt(fields[4]) - 1, Integer.parseInt(fields[5]));


            // Focusing on the origin, record the date and reduce it by 2 days, note the value as dep
            if (deptime > 1200) {
                System.out.println(fields.toString());
                Calendar ndate = date;
                ndate.add(Calendar.DATE, -2);

                valArr.add(String.valueOf(JourneyType.SECOND_LEG)); // orig is the middle point
                valArr.add(fields[0]); // orig
                valArr.add(fields[1]); // dest
                valArr.add(fields[2]); // deptime
                valArr.add(fields[6]); // arrivalDly
                result.add(new Pair<>(
                        fmt.format(ndate.getTime()), // expected starting date
                        valArr.toArray(new String[valArr.size()])));
            }

            valArr = new ArrayList<>();

            // Focusing on the dest, record the date, note the value as arr
            if (deptime < 1200) {
                valArr.add(String.valueOf(JourneyType.FIRST_LEG)); // dest is the middle point
                valArr.add(fields[0]); // orig
                valArr.add(fields[1]); // dest
                valArr.add(fields[2]); // deptime
                valArr.add(fields[6]); // arrivalDly
                result.add(new Pair<>(
                        fmt.format(date.getTime()), // starting date
                        valArr.toArray(new String[valArr.size()])));
            }

            return result;
        }

        @Override
        public void map(LongWritable key, Text val, Context context)
                throws IOException, InterruptedException {
            CSVParser parser = new CSVParser(new StringReader(val.toString()), CSVFormat.DEFAULT);

            for (CSVRecord record: parser) {
                List<String> fields = Lists.newArrayList(record.iterator());
                String[] lineFields = new String[columns.length];
                for (int i = 0; i < columns.length; ++i) {
                    lineFields[i] = fields.get(columns[i]);
                }

                for (Pair<String, String[]> item: computeKeyValues(lineFields)) {
                    context.write(new Text(item.getFirst()), new TextArrayWritable(item.getSecond()));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, TextArrayWritable, TextArrayWritable, IntWritable> {
        public class Journey {
            public String middlepoint;

            public List<String> startpoints;
            public List<String> firstDeptime;
            public List<String> firstArrDelay;

            public List<String> endpoints;
            public List<String> secondDeptime;
            public List<String> secondArrDelay;

            public Journey(String _middlepoint) {
                middlepoint     = _middlepoint;
                startpoints     = new ArrayList<>();
                firstArrDelay   = new ArrayList<>();
                firstDeptime    = new ArrayList<>();
                endpoints       = new ArrayList<>();
                secondArrDelay  = new ArrayList<>();
                secondDeptime   = new ArrayList<>();
            }

            public void addFirstLeg(String airport, String deptime, String arrdelay) {
                startpoints.add(airport);
                firstDeptime.add(deptime);
                firstArrDelay.add(arrdelay);
            }

            public void addSecondLeg(String airport, String deptime, String arrdelay) {
                endpoints.add(airport);
                secondDeptime.add(deptime);
                secondArrDelay.add(arrdelay);
            }

            public ArrayList<Pair<String[], Integer>> getPaths() {
                ArrayList<Pair<String[], Integer>> result = new ArrayList<>();

                for (int i = 0; i < startpoints.size(); ++i) {
                    for (int j = 0; j < endpoints.size(); ++j) {
                        String[] path = new String[3];
                        path[0] = startpoints.get(i) + "@" + firstDeptime.get(i);
                        path[1] = middlepoint + "@" + secondDeptime.get(i);
                        path[2] = endpoints.get(i);

                        Integer arrDelay = Integer.parseInt(firstArrDelay.get(i)) +
                                Integer.parseInt(secondArrDelay.get(i));
                        result.add(new Pair<>(path, arrDelay));
                    }
                }

                return result;
            }
        }

        public class Journeys extends HashMap<String, Journey> {}

        @Override
        public void reduce(Text key, Iterable<TextArrayWritable> vals, Context context)
                throws IOException, InterruptedException {
            Journeys journeys = new Journeys();

            for (TextArrayWritable val: vals) {
                String[] fields = val.toStrings();
                if (fields[0] == String.valueOf(JourneyType.SECOND_LEG)) {
                    // second leg
                    String middlepoint = fields[1];
                    String airport = fields[2];

                    if (!journeys.containsKey(middlepoint)) {
                        journeys.put(middlepoint, new Journey(middlepoint));
                    }
                    journeys.get(middlepoint).addSecondLeg(airport, fields[3], fields[4]);
                } else {
                    // first leg
                    String middlepoint = fields[2];
                    String airport = fields[1];
                    if (!journeys.containsKey(middlepoint)) {
                        journeys.put(middlepoint, new Journey(middlepoint));
                    }
                    journeys.get(middlepoint).addFirstLeg(airport, fields[3], fields[4]);
                }
            }

            for (Journey journey: journeys.values()) {
                for (Pair<String[], Integer> path: journey.getPaths()) {
                    List<String> k = new ArrayList<>();
                    k.add(key.toString());
                    k.addAll(Lists.newArrayList(path.getFirst()));
                    context.write(new TextArrayWritable(k.toArray(new String[k.size()])),
                            new IntWritable(path.getSecond()));
                }
            }
        }
    }

    @Override
    public final int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job;

        conf.set("input", args[0]);
        conf.set("output", args[1]);

        job = new Job(conf, "date-route information collector");
        job.setJarByClass(SuitableRoutes.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArrayWritable.class);
        job.setOutputKeyClass(TextArrayWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

        job.waitForCompletion(true);

        return 0;
    }


}
