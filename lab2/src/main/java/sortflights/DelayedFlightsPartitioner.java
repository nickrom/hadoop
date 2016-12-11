package sortflights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DelayedFlightsPartitioner extends Partitioner<DataFlightsInput, Text> {
    @Override
    public int getPartition(DataFlightsInput dataFlightsInput, Text text, int i) {
        return ((int)dataFlightsInput.getArrDelay() & Integer.MAX_VALUE) % i;
    }
}
