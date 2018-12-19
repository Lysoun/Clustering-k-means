import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Cleaner{
    public static void run(String input, String output, int dimension, String[] columns) throws Exception{
	Configuration conf = new Configuration();
	
	conf.setStrings("columns", columns);
	conf.setInt("dimension", dimension);

	Job job = Job.getInstance(conf, "Cleaner");
	setJob(job);
	FileInputFormat.addInputPath(job, new Path(input));
	FileOutputFormat.setOutputPath(job, new Path(output));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void setJob(Job job){
	job.setNumReduceTasks(1);
	job.setJarByClass(Cleaner.class);
	job.setMapperClass(CleanerMapper.class);
	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setReducerClass(CleanerReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	job.setInputFormatClass(TextInputFormat.class);
    }
}
