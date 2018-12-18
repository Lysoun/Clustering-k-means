import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkClusteringKMeans {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Spark clustering k-means");
		JavaSparkContext context = new JavaSparkContext(conf);
		
	}
	
}
