import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ProjetMapReduce {
	private static List<List<Double>> centroids = new ArrayList<List<Double>>();

	public static class ProjetMapper extends
			Mapper<Object, Text, IntWritable, Text> {
		private int clusterNumber = 1;
	    private int dimension = 1;
	    private List<Integer> columns = new ArrayList<Integer>();
		
		@Override
		protected void setup(Mapper<Object, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			this.clusterNumber = context.getConfiguration().getInt("clusterNumber", 1);
			this.dimension = context.getConfiguration().getInt("dimension", 1);
		        this.columns = parseToInteger(context.getConfiguration().
						      getStrings("columns"));
		}
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Let's check if the line has a float in its *column* column			
			String tokens[]= value.toString().split(",");

			for(Integer column : columns){
			    if(tokens.length < column) return;
			    if(!tokens[column].matches("-?\\d+(\\.\\d+)?")) return;	
			}			

			List<Double> valueVector = new ArrayList<Double>();

			for(Integer column : columns){
			    valueVector.add(Double.parseDouble(tokens[column]));
			}
						
			if(centroids.size() < clusterNumber) {
				int index = centroids.indexOf(valueVector);
				
				if(index == -1){
					centroids.add(valueVector);
					context.write(new IntWritable(centroids.size() - 1), value);				
				} else{
				    context.write(new IntWritable(index), value);
				}
				
			} else{
				context.write(new IntWritable(searchNearestCentroid(valueVector)), value);
			}
			
		}
		
	    private int searchNearestCentroid(List<Double> vector){
			int indexNearest = 0;
			
			Double normMin = norm(centroids.get(0), vector);
			
			for(int i = 0; i < clusterNumber; i++){
				List<Double> centroid = centroids.get(i);
								
				Double norm = norm(centroid, vector);
				if(norm < normMin){
					normMin = norm;
					indexNearest = i;
				}
			}
			
			return indexNearest;
		}
		
		private Double norm(List<Double> vector1, List<Double> vector2){
			Double res = 0.;
			
			// Yes we have the correct number of dimensions for the two vectors.
			// It was checked in map okay.
			
			for(int i = 0; i < vector1.size(); i++){
				res += Math.pow(vector1.get(i) - vector2.get(i), 2.);
			}
			
			return Math.sqrt(res);
		}
	}	

	public static class ProjetReducer extends
			Reducer<IntWritable, Text, Text, IntWritable> {
	    /*		private int clusterNumber = 1;
		private int column = 0;
		
		@Override
		protected void setup(Reducer<IntWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			this.clusterNumber = context.getConfiguration().getInt("clusterNumber", 1);
			this.column = context.getConfiguration().getInt("column", 0);						
			}*/
		
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for(Text value : values){
				context.write(value, key);
			}
		}
	}

    private static List<Integer> parseToInteger(String[] arrayString){
	List<Integer> listInteger = new ArrayList<Integer>();

	for(String e : arrayString){
	    listInteger.add(Integer.parseInt(e));
	}

	return listInteger;
    }

	public static void main(String[] args) throws Exception {
	    int dimension = args.length - 3;
	    String[] columns = new String[dimension];

	    for(int i = 1; i <= dimension; i++){
		columns[i - 1] = args[i + 2];
	    }
	    
	    Configuration conf = new Configuration();
	    conf.setInt("clusterNumber", Integer.parseInt(args[2]));
	    conf.setStrings("columns", columns);
	    conf.setInt("dimension", dimension);
	    
	    Job job = Job.getInstance(conf, "ProjetMapReduce");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(ProjetMapReduce.class);
	    job.setMapperClass(ProjetMapper.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(ProjetReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
