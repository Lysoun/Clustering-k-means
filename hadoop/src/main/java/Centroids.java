import java.util.List;

public class Centroids{
    
    /**
     * Among centroids, searches the nearest to 
     * the vector given
     */
    public static int searchNearestCentroid(List<Double> vector, int clusterNumber, List<List<Double>> centroids){
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
