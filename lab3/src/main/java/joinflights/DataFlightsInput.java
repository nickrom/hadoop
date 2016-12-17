package joinflights;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataFlightsInput implements Writable {

    private Integer airportID;
    private Double arrDelayNew;

    public Integer getAirportID() {
        return airportID;
    }

    public Double getArrDelayNew() {
        return arrDelayNew;
    }

    private static final int INDEX_ARR_DELAY_NEW = 5;
    private static final int INDEX_AIROPORT_ID = 9;

    //unused
    public DataFlightsInput() {
    }

    public DataFlightsInput(String str) {
        String[] tmp = str.split(",");
        int size = tmp.length;
        if (!tmp[size - INDEX_ARR_DELAY_NEW].equals("\"ARR_DELAY_NEW\"") && !tmp[size - INDEX_ARR_DELAY_NEW].isEmpty()) {
            this.arrDelayNew = Double.parseDouble(tmp[size - INDEX_ARR_DELAY_NEW]);
        }
        if (!tmp[size - INDEX_AIROPORT_ID].equals("\"DEST_AIRPORT_ID\"") && !tmp[size - INDEX_AIROPORT_ID].isEmpty()) {
            this.airportID = Integer.parseInt(tmp[size - INDEX_AIROPORT_ID]);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(this.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String[] tmp = dataInput.readLine().split(",");
        arrDelayNew = Double.parseDouble(tmp[0]);
        airportID = Integer.parseInt(tmp[2]);
    }

    @Override
    public String toString() {
        return this.arrDelayNew +
                "," + this.airportID + '\t';
    }
}
