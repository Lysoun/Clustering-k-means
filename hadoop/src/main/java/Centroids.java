import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class Centroids{
    private static final String SPLITTER_COORD = ",";
    private static final String SPLITTER_CENTROIDS = "\n";
    
    /**
     * Writes the centroids given in the output,
     * separator for coordinates is "," and separator between
     * centroids is "\n".
     */
    public static void writeCentroids(List<List<Double>> centroids, OutputStreamWriter output) throws IOException{
	boolean firstLine = true;
	for(List<Double> centroid : centroids){
	    if(firstLine)
		firstLine = false;
	    else{
		output.write(SPLITTER_CENTROIDS);
		output.flush();
	    }
	    
	    writeCentroid(centroid, output);
	}
    }
    
    public static void writeCentroid(List<Double> centroid, OutputStreamWriter output) throws IOException{        
	output.write(centroidToString(centroid));
	output.flush();
    }

    public static String centroidToString(List<Double> centroid){
	String res = "";
	boolean firstCoord = true;
		
	for(Double coordinate: centroid){
	    if(firstCoord)
		firstCoord = false;
	    else
		res += SPLITTER_COORD;
	    
	    res += coordinate.toString();
	}
	
	return res;
	
    }


    public static List<List<Double>> readCentroids(BufferedReader reader) throws IOException{
	Stream<String> lines = reader.lines();
	Iterator<String> iterator = lines.iterator();
	
	List<List<Double>> centroids = new ArrayList<List<Double>>();

	while(iterator.hasNext()){	    
	    List<Double> centroid = new ArrayList<Double>();
	    
	    String line = iterator.next();
	    String tokens[] = line.split(",");

	    for(String token : tokens)
		centroid.add(Double.parseDouble(token));

	    centroids.add(centroid);
	}

	return centroids;

    }

    public static void moveCentroids(BufferedReader source, OutputStreamWriter destination) throws IOException{
	List<List<Double>> centroids = readCentroids(source);
	writeCentroids(centroids, destination);
    }
}
