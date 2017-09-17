package cloud.assignment.twoword;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TwoWordCount {

	public static void main(String[] args) throws Exception {

		// Create a job to perform word counting task
		Job twoWrdJob = Job.getInstance(new Configuration());

		twoWrdJob.setJarByClass(TwoWordCount.class);

		twoWrdJob.setOutputKeyClass(Text.class);
		twoWrdJob.setOutputValueClass(IntWritable.class);

		twoWrdJob.setMapperClass(TwoWordMapper.class);
		twoWrdJob.setReducerClass(TwoWordReducer.class);

		twoWrdJob.setInputFormatClass(TextInputFormat.class);
		twoWrdJob.setOutputFormatClass(TextOutputFormat.class);

		// The first argument has input path and the second argument contain output path
		FileInputFormat.setInputPaths(twoWrdJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(twoWrdJob, new Path(args[1]));

		System.exit(twoWrdJob.waitForCompletion(true) ? 0 : 1);
	}

}
