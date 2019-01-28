import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.io.IOException;

import java.net.URI;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class ClusteringKMeansMapper extends
			      Mapper<Object, Text, IntWritable, Text> {
    private int clusterNumber = 1;
    private int dimension = 1;
    private int columns[] = new int[dimension];
    private List<List<Double>> centroids = new ArrayList<List<Double>>();
    
    @Override
    protected void setup(Mapper<Object, Text, IntWritable, Text>.Context context)
	throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	this.clusterNumber = conf.getInt("clusterNumber", 1);
	this.dimension = conf.getInt("dimension", 1);
	this.columns = Parser.parseToInteger(conf.getStrings("columns"));
       
	URI[] cacheFiles = context.getCacheFiles();
	FileSystem fs = FileSystem.get(conf);
	BufferedReader reader =  new BufferedReader(
						   new InputStreamReader(
									 fs.open(
										 new Path(cacheFiles[0].getPath())),
									 "UTF-8"));
	this.centroids = Centroids.readCentroids(reader);
	reader.close();
    }
    
    public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
	String tokens[] = value.toString().split(",");
	
	for(int column : columns){
	    // Checking if the line does have floats in
	    // all the columns we are going to use
	    if(tokens.length < column) return;
	    if(!tokens[column].matches("-?\\d+(\\.\\d+)?")) return;
	}
	
	// Creating a vector to represent the line
	List<Double> valueVector = new ArrayList<Double>();
	
	for(int column : columns){
	    valueVector.add(Double.parseDouble(tokens[column]));
	}
	
	// Looking for the nearest centroid to the value
	// and writting the index of that centroid as the value's key
	context.write(new IntWritable(searchNearestCentroid(valueVector, clusterNumber, centroids)), value);	    	
    }

        /**
     * Among centroids, searches the nearest to 
     * the vector given
     */
    private static int searchNearestCentroid(List<Double> vector, int clusterNumber, List<List<Double>> centroids){
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
