package csc369;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateByCountry {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    // Mapper: read (country \t count) lines
    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                String country = parts[0].trim();
                int count = Integer.parseInt(parts[1].trim());
                context.write(new Text(country), new IntWritable(count));
            }
        }
    }

    // Reducer: sum all counts per country
    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable val : values) {
                total += val.get();
            }
            context.write(key, new IntWritable(total));
        }
    }
}