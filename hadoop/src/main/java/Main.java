public class Main{
    private static final String OUTPUT_FILE_NAME = "/part-r-00000";
    
    public static void main(String[] args) throws Exception{
	if(args.length < 5){
	    System.out.println("Il vous manque des arguments ! Lancez donc comme ça : "
			       + "\nyarn jar fichier_entree dossier_sortie nombre_iterations nombre_de_clusters"
			       + " colonne1 colonne2...");
	    return;
	}
	
	int dimension = args.length - 4;
	String columns[] = new String[dimension];
	for(int i = 1; i <= dimension; i++)
	    columns[i - 1] = args[i + 3];
	
	String outputFolder = args[1];
	String input = args[0];
	int clusterNumber = Integer.parseInt(args[3]);
	int iterationsNumber = Integer.parseInt(args[2]);

	String outputClustering = outputFolder + "/clustering";
	String centroidFolder = outputFolder + "/centroids";
	
	ClusteringKMeans.run(input, outputClustering, centroidFolder, dimension, columns, clusterNumber, iterationsNumber);
    }
}
