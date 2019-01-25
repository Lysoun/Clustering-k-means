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
	//	System.out.println(s);
	//System.out.println(columns[0] + " " + columns[1]);
	
	for(int column : columns){
	    // Checking if the line does have floats in
	    // all the columns we are going to use
	    if(tokens.length < column){
		//			System.out.println("tokens.length < column");
		return new String[0];

 	    }
	    if(!tokens[column].matches("-?\\d+(\\.\\d+)?")){
		// 				System.out.println("!matches " + column + tokens[column]);
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
	int clusterNumber = Integer.parseInt(args[2]);
	int dimension = args.length - 3;
	int columns[] = new int[dimension];
	int iterationNumber = 1;
	
	for (int i = 0; i < dimension; i++) {
	    columns[i] = Integer.parseInt(args[i + 3]);
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
	List<List<Double>> centroids = cleanTextFile
	    .map(s -> convertToVector(s, dimension, columns))
	    .take(clusterNumber);

	for(List<Double> centroid: centroids){
	    String s = "";
	    for(Double coord : centroid)
		s += coord + " ";
	    System.out.println(s);
	}
		
	// Converting the data to JavaRDDPair<Vector, String>
	JavaRDD<String> points =
	    cleanTextFile
	    .map(s -> s + "," + searchNearestCentroid(convertToVector(s, dimension, columns), clusterNumber, centroids));
		
	points.saveAsTextFile(output);
    }

}
