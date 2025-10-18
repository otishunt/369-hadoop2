package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class ); 

	    job.setReducerClass(UserMessages.JoinReducer.class);

	    job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	}
	else if ("RequestCountByCountry".equalsIgnoreCase(otherArgs[0])) {

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				KeyValueTextInputFormat.class, RequestCountByCountry.HostnameMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
				TextInputFormat.class, RequestCountByCountry.AccessMapper.class);

		job.setReducerClass(RequestCountByCountry.JoinReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(RequestCountByCountry.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(RequestCountByCountry.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	}  else if ("CountURLs".equalsIgnoreCase(otherArgs[0])) {

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				KeyValueTextInputFormat.class, CountURLs.HostnameMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
				TextInputFormat.class, CountURLs.AccessMapper.class);

		job.setReducerClass(CountURLs.JoinReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(CountURLs.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CountURLs.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	}	else if ("CountURLs3".equalsIgnoreCase(otherArgs[0])) {

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				KeyValueTextInputFormat.class, CountURLs3.HostnameMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
				TextInputFormat.class, CountURLs3.AccessMapper.class);

		job.setReducerClass(CountURLs3.JoinReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(CountURLs3.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CountURLs3.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	}	else if ("AggregateByCountry".equalsIgnoreCase(otherArgs[0])) {
		job.setMapperClass(AggregateByCountry.MapperImpl.class);
		job.setReducerClass(AggregateByCountry.ReducerImpl.class);

		job.setOutputKeyClass(AggregateByCountry.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(AggregateByCountry.OUTPUT_VALUE_CLASS);

		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}	else if ("AggregateByURL".equalsIgnoreCase(otherArgs[0])) {
			job.setMapperClass(AggregateByURL.MapperImpl.class);
			job.setReducerClass(AggregateByURL.ReducerImpl.class);

			job.setOutputKeyClass(AggregateByURL.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(AggregateByURL.OUTPUT_VALUE_CLASS);

			FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} 	else if ("AggregateByCountryURL".equalsIgnoreCase(otherArgs[0])) {
		job.setMapperClass(AggregateByCountryURL.MapperImpl.class);
		job.setReducerClass(AggregateByCountryURL.ReducerImpl.class);

		job.setOutputKeyClass(AggregateByCountryURL.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(AggregateByCountryURL.OUTPUT_VALUE_CLASS);

		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}	else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog2".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(AccessLog2.ReducerImpl.class);
		job.setMapperClass(AccessLog2.MapperImpl.class);
		job.setOutputKeyClass(AccessLog2.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(AccessLog2.OUTPUT_VALUE_CLASS);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog2URL".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(AccessLog2URL.ReducerImpl.class);
		job.setMapperClass(AccessLog2URL.MapperImpl.class);
		job.setOutputKeyClass(AccessLog2URL.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(AccessLog2URL.OUTPUT_VALUE_CLASS);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}	else if ("SortByCountryThenCount".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(SortByCountryThenCount.ReducerImpl.class);
		job.setMapperClass(SortByCountryThenCount.MapperImpl.class);
		job.setOutputKeyClass(SortByCountryThenCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(SortByCountryThenCount.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
