package joinflights;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by user on 17.12.16.
 */
public class JoinFlightsComparator extends WritableComparator {
    protected JoinFlightsComparator() {
        super(FlightsAirportJoinerKey.class, true);
    }

    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        FlightsAirportJoinerKey f = (FlightsAirportJoinerKey) first;
        FlightsAirportJoinerKey s = (FlightsAirportJoinerKey) second;
        if(f.getAirportID() == null || s.getAirportID() == null)
            return 0;
        return f.getAirportID() - s.getAirportID();
    }
}
