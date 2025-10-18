package csc369;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SortByCountryThenCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");
            if (parts.length == 3) {
                String country = parts[0].trim();
                String url = parts[1].trim();
                String count = parts[2].trim();
                context.write(new Text(country), new Text(url + "\t" + count));
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text country, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<Map.Entry<String, Integer>> urlCounts = new ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts.length == 2) {
                    String url = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    urlCounts.add(new AbstractMap.SimpleEntry<>(url, count));
                }
            }

            urlCounts.sort((a, b) -> b.getValue().compareTo(a.getValue()));

            for (Map.Entry<String, Integer> entry : urlCounts) {
                context.write(country, new Text(entry.getKey() + "\t" + entry.getValue()));
            }
        }
    }
}
