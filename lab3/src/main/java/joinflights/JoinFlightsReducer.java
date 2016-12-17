package joinflights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by user on 17.12.16.
 */
public class JoinFlightsReducer extends Reducer<FlightsAirportJoinerKey, Text, Integer, Text> {
    @Override
    protected void reduce(FlightsAirportJoinerKey key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
        Long count = 0L;
        Double min = Double.MAX_VALUE, max = 0.0, sum = 0.0, curValue;
        String value = "";
        for (Text v : vals) {
            if(count == 0){
                value = v.toString();
            }
            else{
                curValue = Double.parseDouble(v.toString());
                if (curValue < min) {
                    min = curValue;
                }
                if (curValue > max) {
                    max = curValue;
                }
                sum += curValue;
            }
            count++;
        }

        if (count > 1) {
            context.write(key.getAirportID(), new Text(value + ", min = " + min.toString() + ", max = " + max.toString() +
                    ", average =" + ((Double) (sum / count)).toString()));
        }
    }
}
