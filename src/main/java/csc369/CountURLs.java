package csc369;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountURLs {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

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

    public static class AccessMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] sa = value.toString().trim().split("\\s+");
            if (sa.length > 6) {
                String hostname = sa[0].trim();
                String url = sa[6].trim();
                context.write(new Text(hostname), new Text("B\t" + url));
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String country = null;
            java.util.List<String> urls = new java.util.ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts[0].equals("A")) {
                    country = parts[1];
                } else if (parts[0].equals("B") && parts.length > 1) {
                    urls.add(parts[1]);
                }
            }

            if (country != null) {
                for (String url : urls) {
                    context.write(new Text(country + "\t" + url), one);
                }
            }
        }
    }
}
