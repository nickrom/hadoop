package joinflights;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by user on 17.12.16.
 */
public class AirportNameMapper extends Mapper<LongWritable, Text, FlightsAirportJoinerKey, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        DataAirportsInput airport = new DataAirportsInput(value.toString());
        FlightsAirportJoinerKey airportKey = new FlightsAirportJoinerKey(airport.getCode(), false);
        if(airport.getName() != null) {
            Text airportValue = new Text(airport.getName());
            context.write(airportKey, airportValue);
        }
    }
}
