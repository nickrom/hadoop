package sortflights;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DelayedFlightsComparator extends WritableComparator {
    protected DelayedFlightsComparator() {
        super(DataFlightsInput.class, true);
    }

    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        DataFlightsInput f = (DataFlightsInput) first;
        DataFlightsInput s = (DataFlightsInput) second;
        return ((int)f.getArrDelay()-(int)s.getArrDelay());
    }

}
