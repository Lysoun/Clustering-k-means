import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class CleanerMapper extends
			      Mapper<Object, Text, NullWritable, Text> {
    private int dimension = 1;
    private int columns[] = new int[dimension];

    @Override
    protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException{
	this.dimension = context.getConfiguration().getInt("dimension", 1);
	this.columns = Parser.parseToInteger(context.getConfiguration().getStrings("columns"));	
    }

    @Override
    public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException{
	String tokens[] = value.toString().split(",");
	
	for(int column : columns){
	    // Checking if the line does have floats in
	    // all the columns we are going to use
	    if(tokens.length < column) return;
	    if(!tokens[column].matches("-?\\d+(\\.\\d+)?")) return;
	}

	context.write(NullWritable.get(), value);	
    }
}
