import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class CleanerReducer extends
			       Reducer<IntWritable, Text, Text, NullWritable> {
    
    public void reduce(IntWritable key, Iterable<Text> values,
		       Context context) throws IOException, InterruptedException {	
	// Writing the values
	for(Text value : values){
	    context.write(value, NullWritable.get());
	}
    }
}
