import java.util.ArrayList;
import java.util.List;

public class Parser{
    public static List<Integer> parseToInteger(String[] arrayString){
	List<Integer> listInteger = new ArrayList<Integer>();
	
	for(String e : arrayString){
	    listInteger.add(Integer.parseInt(e));
	}
	
	return listInteger;
    }    
}
