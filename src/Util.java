import java.util.Iterator;
import java.util.List;


public class Util {

	public static int max(int x, int y)
	{
		if(x>y)
			return x;
		else
			return y;
	}
	
	public static int randomInRange(int minimum,int maximum)
	{
		int randomNum = minimum + (int)(Math.random()*maximum);
		return randomNum;
	}
	
	public static int[] convertIntegers(List<Integer> integers)
	{
	    int[] ret = new int[integers.size()];
	    Iterator<Integer> iterator = integers.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().intValue();
	    }
	    return ret;
	}
}
