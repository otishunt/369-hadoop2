package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AccessLog2URL {

    public static final Class OUTPUT_KEY_CLASS = LongWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split("\t");
            String country = sa[0].trim();
            String url = sa[1].trim();
            long count = Long.parseLong(sa[2].trim());

            context.write(new LongWritable(count), new Text(country + "\t" + url));
        }
    }


    public static class ReducerImpl extends Reducer<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void reduce(LongWritable count, Iterable<Text> hostnames,
                              Context context) throws IOException, InterruptedException {

            Iterator<Text> itr = hostnames.iterator();
            while (itr.hasNext()){
                context.write(itr.next(), count);
            }
        }
    }

}
