package csc369;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RequestCountByCountry {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    // Mapper for hostname_country.csv
    public static class HostnameMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String hostname = key.toString().trim();
            String country = value.toString().trim();
            if (hostname.equalsIgnoreCase("hostname")) return; // skip header
            context.write(new Text(hostname), new Text("A\t" + country));
        }
    }

    // Mapper for access log file
    public static class AccessMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] text = value.toString().split(" ");
            if (text.length > 0) {
                String host = text[0];
                context.write(new Text(host), new Text("B\t1"));
            }
        }
    }

    // Reducer for join
    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable> {
        private java.util.Map<String, Integer> countryTotals = new java.util.HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String country = null;
            int count = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts[0].equals("A")) {
                    country = parts[1];
                } else if (parts[0].equals("B")) {
                    count += 1;
                }
            }

            if (country != null && count > 0) {
                context.write(new Text(country), new IntWritable(count));
            }
        }
    }
}
