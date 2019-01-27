import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;

import org.apache.spark.SparkConf;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkClusteringKMeans {
    public static List<Double> convertToVector(String s, int dimension,
					       int[] columns) {
	String[] tokens = s.split(",");
	List<Double> res = new ArrayList<Double>();
	
	for (int i = 0; i < dimension; i++)
	    res.add(Double.parseDouble(tokens[columns[i]]));
	return res;
    }
    
    public static Integer searchNearestCentroid(List<Double> vector,
			int clusterNumber, List<List<Double>> centroids) {
	int indexNearest = 0;
	
	Double diffMin = norm_difference(centroids.get(0), vector);
	
	for (int i = 0; i < clusterNumber; i++) {
	    List<Double> centroid = centroids.get(i);
	    
	    Double diff = norm_difference(centroid, vector);
	    if (diff < diffMin) {
		diffMin = diff;
		indexNearest = i;
	    }
	}
	
	return new Integer(indexNearest);
	
    }
    
    public static Double norm_difference(List<Double> vector1,
					 List<Double> vector2) {
	Double res = 0.;
	
	for (int i = 0; i < vector1.size(); i++) {
	    res += Math.pow(vector1.get(i) - vector2.get(i), 2.);
	}
	
	return Math.sqrt(res);
    }

    public static String[] validLine(String s, int[] columns){
	String tokens[] = s.toString().split(",");
	
	for(int column : columns){
	    // Checking if the line does have floats in
	    // all the columns we are going to use
	    if(tokens.length < column){
		return new String[0];
 	    }
	    if(!tokens[column].matches("-?\\d+(\\.\\d+)?")){
		return new String[0];
	    }
	}
	
	String res[] = new String[1];
	res[0] = s;
	
	return res;
    }
    
    public static void main(String[] args) {
	String input = args[0];
	String output = args[1];
	final int clusterNumber = Integer.parseInt(args[2]);
	final int dimension = args.length - 4;
	final int columns[] = new int[dimension];
	final int iterationNumber = Integer.parseInt(args[3]);
	
	for (int i = 0; i < dimension; i++) {
	    columns[i] = Integer.parseInt(args[i + 4]);
	}
	
	// Create a SparkContext to initialize
	SparkConf conf = new SparkConf().setAppName(
						    "Clustering K Means");
	
	// Create a Java version of the Spark Context
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	// Load the text into a Spark RDD, which is a distributed representation
	// of each line of text
	JavaRDD<String> textFile = sc.textFile(args[0]);
	
	// Clean the text to have valid data
	JavaRDD<String> cleanTextFile =
	    textFile
	    .distinct()
	    .flatMap(s -> Arrays.asList(validLine(s, columns)).iterator()
		);
	    // Removes the
	    // duplications //
	    // Removes the
	    // invalid lines

	//cleanTextFile.foreach(l -> System.out.println(l));
		
	
	// Taking the text's clusterNumber first lines as centroids
	List<List<Double>> centroids =
	    cleanTextFile
	    .map(s -> convertToVector(s, dimension, columns))
	    .take(clusterNumber);

	/*	for(List<Double> centroid: centroids){
	    String s = "";
	    for(Double coord : centroid)
		s += coord + " ";
		}*/

	JavaRDD<String> dataSource = cleanTextFile;
	JavaPairRDD<Integer, String> points = null;

	System.out.println("Allez, je vais le faire " + iterationNumber + " fois ! ");

	for(int i = 0; i < iterationNumber; i++){
	    System.out.println("Et de " + (i + 1) +" ! ");
	    final List<List<Double>> currentCentroids = new ArrayList<List<Double>>(centroids);
	    points =
		dataSource
		.mapToPair(s ->
			   {
			       Integer nearest = searchNearestCentroid(convertToVector(s, dimension, columns), clusterNumber, currentCentroids);
			       return new Tuple2<Integer, String>(nearest, s + "," + nearest);
			   });	    
	    
	    JavaRDD<List<Double>> newCentroids =
		points
		.groupByKey()
		.mapValues(lines -> {
			int count = 0;
			List<Double> newCentroid = new ArrayList<Double>();

			for(int c = 0; c < dimension; c++)
			    newCentroid.add(0.);
			
			for(String line: lines){
			    List<Double> vector = convertToVector(line, dimension, columns);
			    
			    for(int c = 0; c < dimension; c++){
				newCentroid.set(c, vector.get(c) + newCentroid.get(c));
			    }
			    count++;
			}
			
			for(int c = 0; c < dimension; c++){
			    newCentroid.set(c, newCentroid.get(c) / count);
			}

			return newCentroid;
		    }).values();
	    
	    centroids = newCentroids.collect();
	    System.out.println("Nous voilà maintenant avec " + centroids.size() + " centroïdes !");
	    dataSource = points.values();
	}
	
	points
	    .values()
	    .saveAsTextFile(output);	
    }   
}
