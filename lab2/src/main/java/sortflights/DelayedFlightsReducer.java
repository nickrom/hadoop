package sortflights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DelayedFlightsReducer extends Reducer<DataFlightsInput, Text, DataFlightsInput, Text>{
    @Override
    protected void reduce(DataFlightsInput key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text val : values)
            context.write(key, val);
    }
}
