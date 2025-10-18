package csc369;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateByURL {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                String url = parts[0].trim();
                String country = parts[1].trim();
                context.write(new Text(url), new Text(country));
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text url, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<String> countries = new TreeSet<>();

            for (Text val : values) {
                countries.add(val.toString().trim());
            }

            String joinedCountries = String.join(", ", countries);

            context.write(url, new Text(joinedCountries));
        }
    }
}
