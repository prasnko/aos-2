import java.util.Comparator;

public class TotalOrderingComparator implements Comparator<String[]>
{
    @Override
    public int compare(String[] x, String[] y)
    {
        // Assume neither string is null. Real code should
        // probably be more robust
        if (Integer.parseInt(x[0]) < Integer.parseInt(y[0]))
        {
            return -1;
        }
        else if (Integer.parseInt(x[0]) < Integer.parseInt(y[0]))
        {
            return 1;
        }
        else if (Integer.parseInt(x[0]) == Integer.parseInt(y[0]))
        {
        	if (Integer.parseInt(x[1]) < Integer.parseInt(y[1]))
            {
                return -1;
            }
            else if (Integer.parseInt(x[1]) < Integer.parseInt(y[1]))
            {
                return 1;
            }
        }
        return 0;
    }
}