import java.io.IOException;
import java.io.OutputStreamWriter;

import java.net.URI;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ClusteringKMeansReducer extends
			       Reducer<IntWritable, Text, WritableComparable, Writable> {
    private static final String SPLITTER = ",";
    
    private List<Double> newCentroid;
    private int dimension = 1;
    private int columns[] = new int[dimension];
    private MultipleOutputs outputs;

    @Override
    protected void setup(Reducer<IntWritable, Text, WritableComparable, Writable>.Context context) throws IOException, InterruptedException{
	Configuration conf = context.getConfiguration();
	this.dimension = conf.getInt("dimension", 1);
	this.columns = Parser.parseToInteger(conf.getStrings("columns"));
	outputs = new MultipleOutputs(context);
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,
		       Context context) throws IOException, InterruptedException {
	// Initialise newCentroid
	newCentroid = new ArrayList<Double>();

	for(int i = 0; i < dimension; i++)
	    newCentroid.add(0.0);
	
	int numberValues = 0;
	for(Text value : values){
	    
	    // Adding the value's coordinates in newCentroid
	    String tokens[] = value.toString().split(SPLITTER);

	    for(int j = 0; j < columns.length; j++)
		newCentroid.set(j, newCentroid.get(j) + Double.parseDouble(tokens[columns[j]]));
	    
	    // Writing the value
	    outputs.write("clustering", value, key);
	    numberValues++;
	}

	// Computing the mean
	for(int i = 0; i < dimension;i ++)
	    newCentroid.set(i, newCentroid.get(i) / numberValues);

        outputs.write("newCentroids", NullWritable.get(), new Text(Centroids.centroidToString(newCentroid)));
    }

    @Override
    protected void cleanup(Reducer<IntWritable, Text, WritableComparable, Writable>.Context context) throws IOException, InterruptedException{
	outputs.close();
    }
}
