import java.util.ArrayList;
import java.util.List;

public class Parser{
    public static int[] parseToInteger(String[] arrayString){
	int res[] = new int[arrayString.length];
	
	for(int i = 0; i < arrayString.length; i++)
	    res[i] = Integer.parseInt(arrayString[i]);	
	
	return res;
    }    
}
