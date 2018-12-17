import java.util.Comparator;
import java.util.List;

public class ComparatorVectors implements Comparator<List<Double>>{
    private Double epsilon;
    
    public ComparatorVectors(Double epsilon){
	this.epsilon = new Double(epsilon);
    }
    
    @Override
    public int compare(List<Double> v1, List<Double> v2){
	int res = 0;
	int i = 0;
	List<Double> v;

        if(v1.size() != v2.size())
	    throw new Exception("You can only compare vectors that have the same size.");

	while(res == 0 && i < v1.size()){
	    if(v1.get(i) > v2.get(i))
		res = 1;
	    else if(v1.get(i) < v2.get(i))
		res = -1;
	    else
		i++;
	}

	return res;
	    
    }

    @Override
    public boolean equals(List<Double> v1, List<Double> v2){
	return v1.equals(v2);
    }
}
