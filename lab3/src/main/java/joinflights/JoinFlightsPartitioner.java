package joinflights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by user on 17.12.16.
 */
public class JoinFlightsPartitioner extends Partitioner<FlightsAirportJoinerKey, Text> {
    @Override
    public int getPartition(FlightsAirportJoinerKey flightdata, Text text, int i) {
        if(flightdata.getAirportID() != null)
            return (flightdata.getAirportID() & Integer.MAX_VALUE) % i;
        return 1;
    }
}
