import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import java.net.URI;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ClusteringKMeans {
    private static final String CENTROID_FILE_NAME = "centroid.txt";
    private static final String NEW_CENTROID_FILE_NAME = "newCentroids-r-00000";
    private static final String SPLITTER = ",";
                    
    public static void run(String input, String outputFolder, String centroidFolder, int dimension, String[] columns, int clusterNumber, int iterationsNumber) throws Exception{
	int iteration = 0;
	
	// Setting the configuration for the job
	Configuration conf = new Configuration();
	conf.setInt("clusterNumber", clusterNumber);
	conf.setStrings("columns", columns);
	conf.setInt("dimension", dimension);

	String centroidsFile = centroidFolder + "/" + CENTROID_FILE_NAME;
	URI centroidsURI = new URI(centroidsFile + "#" + CENTROID_FILE_NAME);

	FileSystem fs = FileSystem.get(conf);

	// Creating the cache files!
	fs.createNewFile(new Path(centroidsFile));
			
	// Writing the first centroids in the centroids cache file
	FSDataInputStream inputStream = fs.open(new Path(input));
	BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
	List<List<Double>> centroids = getFirstCentroids(br, Parser.parseToInteger(columns), clusterNumber);

	FSDataOutputStream outputStream = fs.create(new Path(centroidsFile));
	OutputStreamWriter outputCentroids = new OutputStreamWriter(outputStream);
	Centroids.writeCentroids(centroids, outputCentroids);
	outputCentroids.close();

	BufferedReader readerNewCentroids;
	String newCentroidsFile, currentOutput;
	
	while(iteration < iterationsNumber){	    
	    // Setting the job
	    Job job = Job.getInstance(conf, "ProjetMapReduce");
	    job.addCacheFile(centroidsURI);
	    
	    setJob(job);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    currentOutput = outputFolder + "-it-" + iteration;
	    FileOutputFormat.setOutputPath(job, new Path(currentOutput));	    	    	    
	    
	    // Running the job
	    job.waitForCompletion(true);

	    newCentroidsFile = currentOutput + "/" + NEW_CENTROID_FILE_NAME;	    
	    centroidsURI = new URI(newCentroidsFile);
	    
	    iteration++;
	}

    }

    private static List<List<Double>> getFirstCentroids(BufferedReader reader, int[] columns, int clusterNumber){
	List<List<Double>> centroids = new ArrayList<List<Double>>();

	Stream<String> lines = reader.lines();
	Iterator<String> iterator = lines.iterator();
	int i = 0;
	while(i < clusterNumber){
	    String line = iterator.next();

	    if(isValidLine(line, columns)){
		String tokens[] = line.split(SPLITTER);
		List<Double> centroid = new ArrayList<Double>();
		
		for(int column : columns)
		    centroid.add(Double.parseDouble(tokens[column]));

		if(!centroids.contains(centroid)){
		    centroids.add(centroid);
		    i++;
		}
	    }
	}

	return centroids;
    }

    private static boolean isValidLine(String line, int[] columns){
	String tokens[] = line.split(SPLITTER);
	
	boolean res = true;
	int i = 0;
	
	while(res && i < columns.length){
	    int column = columns[i];
	    // Checking if the line does have floats in
	    // all the columns we are going to use
	    if(tokens.length < column)
		res = false;
	    if(!tokens[column].matches("-?\\d+(\\.\\d+)?"))
		res = false;
	    i++;
	}
	
	return res;
    }
    
    private static void setJob(Job job){
	job.setNumReduceTasks(1);
	job.setJarByClass(ClusteringKMeans.class);
	job.setMapperClass(ClusteringKMeansMapper.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setReducerClass(ClusteringKMeansReducer.class);

	MultipleOutputs.addNamedOutput(job, "clustering", TextOutputFormat.class, Text.class, IntWritable.class);
	MultipleOutputs.addNamedOutput(job, "newCentroids", TextOutputFormat.class, NullWritable.class, Text.class);
	
	job.setInputFormatClass(TextInputFormat.class);
    }
}
