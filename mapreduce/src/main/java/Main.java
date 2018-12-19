public class Main{
    private static final String OUTPUT_FILE_NAME = "/part-r-00000";
    
    public static void main(String[] args) throws Exception{
	if(args.length < 4){
	    System.out.println("Il vous manque des arguments ! Lancez donc comme ça : "
			       + "\nyarn jar fichier_entree dossier_sortie nombre_de_clusters"
			       + " colonne1 colonne2...");
	    return;
	}
	
	int dimension = args.length - 3;
	String columns[] = new String[dimension];
	for(int i = 1; i <= dimension; i++)
	    columns[i - 1] = args[i + 2];
	
	String outputFolder = args[1];
	String input = args[0];
	int clusterNumber = Integer.parseInt(args[2]);

	String outputCleaner = outputFolder + "-clean";

	Cleaner.run(input, outputCleaner, dimension, columns);

	String outputClustering = outputFolder + "-clustering";
	//	ClusteringKMeans.run(outputCleaner + OUTPUT_FILE_NAME, outputClustering, dimension, columns, clusterNumber);
    }
}
