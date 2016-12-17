package joinflights;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayedFlightsMapper extends Mapper<LongWritable, Text, FlightsAirportJoinerKey, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        DataFlightsInput flight = new DataFlightsInput(value.toString());
        if (flight.getArrDelayNew() == null)
            return;
        FlightsAirportJoinerKey flightKey = new FlightsAirportJoinerKey(flight.getAirportID(), true);
        Text flightValue = new Text(flight.getArrDelayNew().toString());
        if (!flight.getArrDelayNew().equals(0.0)) {
            context.write(flightKey, flightValue);
        }
    }
}
