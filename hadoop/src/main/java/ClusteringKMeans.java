import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ClusteringKMeans {
    public final static String JOB_NAME = "ClusteringKMeans";
    public final static Double EPSILON = 0.1;
    
    private static List<List<Double>> centroids = new ArrayList<List<Double>>();
    private static List<List<Double>> newCentroids = new ArrayList<List<Double>>();
    private static int iteration = 0;
    private static int clusterNumber = 1;
    private static int dimension = 1;
    private static List<Integer> columns = new ArrayList<Integer>();
    
    public static class ProjetMapper implements Mapper<LongWritable, Text, IntWritable, Text> {
	@Override
	public void configure(JobConf job){
	    // We need to implement this but
	    // we do not have anything to do here...
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
	    throws IOException {
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
		    output.collect(new IntWritable(centroids.size() - 1), value);				
		} else{
		    // When it is, just writting it and its cluster number
		    // as its key
		    output.collect(new IntWritable(index), value);
		}
		
	    } else{
		// Looking for the nearest centroid to the value
		// and writting the index of that centroid as the value's key
		output.collect(new IntWritable(Centroids.searchNearestCentroid(valueVector, clusterNumber)), value);
	    }
	    
	}

	@Override
	public void close() throws IOException{}
    }
    
    public static class Centroids{

	/**
	 * Among centroids, searches the nearest to 
	 * the vector given
	 */
	public static int searchNearestCentroid(List<Double> vector, int clusterNumber){
	    int indexNearest = 0;
	    
	    Double diffMin = normDifference(centroids.get(0), vector);
	    
	    for(int i = 0; i < clusterNumber; i++){
		List<Double> centroid = centroids.get(i);
		
		Double diff = normDifference(centroid, vector);
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
	private static Double normDifference(List<Double> vector1, List<Double> vector2){
	    Double res = 0.;
	    
	    // No need to check the dimensions of the vectors,
	    // it's assumed that they're correct
	    
	    for(int i = 0; i < vector1.size(); i++){
		res += Math.pow(vector1.get(i) - vector2.get(i), 2.);
	    }
	    
	    return Math.sqrt(res);
	}

    }

    
    public static class ProjetReducer implements
					  Reducer<IntWritable, Text, Text, IntWritable> {
	@Override
	public void configure(JobConf job){ }
	
	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			   OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
	    List<Double> newCentroid = new ArrayList<Double>();
	    Double numberValues = 0.;

	    // Initialise newCentroid with zeroes !
	    for(int i = 0; i < dimension; i++)
		newCentroid.add(0.);
	    
	    
	    while(values.hasNext()){
		Text value = values.next();

		// Adding the value's coordinates into the newCentroid
		String tokens[] = value.toString().split(",");
		for(int i = 0; i < dimension; i++)
		    newCentroid.set(i,
				    newCentroid.get(i) +
				    Double.parseDouble(tokens[columns.get(i)]));
		
		numberValues = numberValues + 1.;
		
		// Writing the value
		output.collect(value, key);
	    }

	    // Computing the mean for each column in newCentroid
	    for(int i = 0; i < newCentroid.size(); i++)
		newCentroid.set(i, newCentroid.get(i) / numberValues);

	    newCentroids.add(newCentroid);
	}

	@Override
	public void close(){ }
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
	// Getting and setting the configuration
	int dimension = args.length - 3;
	String[] columns = new String[dimension];

	for(int i = 1; i <= dimension; i++){
	    columns[i - 1] = args[i + 2];
	}

	ClusteringKMeans.columns = Parser.parseToInteger(columns);
	ClusteringKMeans.dimension = dimension;
	ClusteringKMeans.clusterNumber = Integer.parseInt(args[2]);
	

	// Iterating until convergence
	boolean isDone = false;
	ComparatorVectors comparator = new ComparatorVectors(EPSILON);

	while(!isDone){
	    JobConf jobConf = new JobConf(ClusteringKMeans.class);
	    setJobConf(jobConf);
	    FileInputFormat.addInputPath(jobConf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

	    JobClient.runJob(jobConf);	    
	    isDone = compareCentroids(comparator);
	    centroids = new ArrayList<List<Double>>(newCentroids);
	    newCentroids.clear();
	    
	    ++iteration;
	}		      
    }

    public static void setJobConf(JobConf job){
	job.setJobName(JOB_NAME);
	job.setMapperClass(ProjetMapper.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setReducerClass(ProjetReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setOutputFormat(TextOutputFormat.class);
	job.setInputFormat(TextInputFormat.class);	
    }

    public static boolean compareCentroids(ComparatorVectors comparator){
	centroids.sort(comparator);
	newCentroids.sort(comparator);

	boolean res = true;
	int i = 0;

	while(res && i < dimension){
	    if(comparator.compare(centroids.get(i), newCentroids.get(i)) != 0)
		res = false;
	    ++i;
	}

	return res;
    }
}
