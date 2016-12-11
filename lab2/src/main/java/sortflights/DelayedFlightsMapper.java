package sortflights;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayedFlightsMapper extends Mapper<LongWritable, Text, DataFlightsInput, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            DataFlightsInput flight = new DataFlightsInput(value.toString());
            if (flight.isCancelled() || flight.getArrDelay() < 0)
                context.write(flight, value);
    }
}
