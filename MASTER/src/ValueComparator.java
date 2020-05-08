import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

public class ValueComparator implements Comparator<Map.Entry<String, Integer>> {

	@Override
	public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
		// TODO Auto-generated method stub
		return arg0.getValue().compareTo(arg1.getValue());
	}
	
}
