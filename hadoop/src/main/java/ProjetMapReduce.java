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
    private static List<List<Double>> new_centroids = new ArrayList<List<Double>>();
    
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
	    this.columns = Parser.parseToInteger(context.getConfiguration().
						 getStrings("columns"));
	}
	
	public void map(Object key, Text value, Context context)
	    throws IOException, InterruptedException {
	    String tokens[]= value.toString().split(",");
	    
	    for(Integer column : columns){
		// Checking if the line does have floats in
		// all the columns we are going to use
		if(tokens.length < column) return;
		if(!tokens[column].matches("-?\\d+(\\.\\d+)?")) return;	
	    }			

	    // Creating a vector to represent the line
	    List<Double> valueVector = new ArrayList<Double>();
	    
	    for(Integer column : columns){
		valueVector.add(Double.parseDouble(tokens[column]));
	    }
	    
	    if(centroids.size() < clusterNumber) {
		// When there is not enough centroids, checking if
		// this value is already a centroid
		int index = centroids.indexOf(valueVector);
		
		if(index == -1){
		    // When it's not, adding it
		    centroids.add(valueVector);
		    context.write(new IntWritable(centroids.size() - 1), value);				
		} else{
		    // When it is, just writting it and its cluster number
		    // as its key
		    context.write(new IntWritable(index), value);
		}
		
	    } else{
		// Looking for the nearest centroid to the value
		// and writting the index of that centroid as the value's key
		context.write(new IntWritable(Centroids.searchNearestCentroid(valueVector, clusterNumber)), value);
	    }
	    
	}
    }
    
    public static class Centroids{

	/**
	 * Among centroids, searches the nearest to 
	 * the vector given
	 */
	public static int searchNearestCentroid(List<Double> vector, int clusterNumber){
	    int indexNearest = 0;
	    
	    Double diffMin = norm_difference(centroids.get(0), vector);
	    
	    for(int i = 0; i < clusterNumber; i++){
		List<Double> centroid = centroids.get(i);
		
		Double diff = norm_difference(centroid, vector);
		if(diff < diffMin){
		    diffMin = diff;
		    indexNearest = i;
		}
	    }
	    
	    return indexNearest;
	}
	
	/**
	 * Computes the difference in the norms of
	 * the two vectors given 
	 */
	private static Double norm_difference(List<Double> vector1, List<Double> vector2){
	    Double res = 0.;
	    
	    // No need to check the dimensions of the vectors,
	    // it's assumed that they're correct
	    
	    for(int i = 0; i < vector1.size(); i++){
		res += Math.pow(vector1.get(i) - vector2.get(i), 2.);
	    }
	    
	    return Math.sqrt(res);
	}
    }
    
    public static class ProjetReducer extends
					  Reducer<IntWritable, Text, Text, IntWritable> {
	
	public void reduce(IntWritable key, Iterable<Text> values,
			   Context context) throws IOException, InterruptedException {
	    // Computing the new center for this cluster

	    // Writing the values
	    for(Text value : values){
		context.write(value, key);
	    }
	}
    }
    
    
    public static void main(String[] args) throws Exception {
	if(args.length < 5){
	    System.out.println("Il vous manque des arguments ! Lancez donc comme ça : "
				   + "\nyarn jar fichier_entree dossier_sortie nombre_de_clusters"
			       + " colonne1 colonne2...");
	    return;
	}

	run(args);
    }

    public static void run(String[] args) throws Exception{
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
	setJob(job);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static void setJob(Job job){
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
    }
}
