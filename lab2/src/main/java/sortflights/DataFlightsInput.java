package sortflights;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataFlightsInput implements WritableComparable {

    private double arrDelay;
    private double airportId;
    private double airTime;
    private boolean cancelled;

    private static final int INDEX_ARR_DELAY = 6;
    private static final int INDEX_AIROPORT_ID = 9;
    private static final int INDEX_AIR_TIME = 2;
    private static final int INDEX_CANCELLED = 4;

    //unused
    public DataFlightsInput() {
    }

    public DataFlightsInput(String str) {
        String[] tmp = str.split(",");
        int size = tmp.length;
        if (!tmp[size - INDEX_ARR_DELAY].equals("\"ARR_DELAY\"") && !tmp[size - INDEX_ARR_DELAY].isEmpty()) {
            this.arrDelay = Double.parseDouble(tmp[size - INDEX_ARR_DELAY]);
        }
        if (!tmp[size - INDEX_AIROPORT_ID].equals("\"DEST_AIRPORT_ID\"") && !tmp[size - INDEX_AIROPORT_ID].isEmpty()) {
            this.airportId = Double.parseDouble(tmp[size - INDEX_AIROPORT_ID]);
        }
        if (!tmp[size - INDEX_AIR_TIME].equals("\"AIR_TIME\"") && !tmp[size - INDEX_AIR_TIME].isEmpty()) {
            this.airTime = Double.parseDouble(tmp[size - INDEX_AIR_TIME]);
        }
        if (!tmp[size - INDEX_CANCELLED].equals("\"CANCELLED\"") && !tmp[size - INDEX_CANCELLED].isEmpty()) {
            this.cancelled = tmp[size - INDEX_CANCELLED].equals("1.00");
        }
    }

    @Override
    public int compareTo(@SuppressWarnings("NullableProblems") Object o) {
        if (!(o instanceof DataFlightsInput)) {
            System.out.println("Error: Incomparable types");
            return -1;
        }
        DataFlightsInput second = (DataFlightsInput) o;
        if (this.cancelled != second.cancelled)
            return (this.cancelled ? -1 : 1);
        else if (this.arrDelay != second.arrDelay)
            return (this.arrDelay - second.arrDelay > 0 ? -1 : 1);
        else if (this.airportId != second.airportId)
            return (this.airportId - second.airportId > 0 ? 1 : -1);
        else if (this.airTime != second.airTime)
            return (this.airTime - second.airTime > 0 ? 1 : -1);
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(this.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String[] tmp = dataInput.readLine().split(",");
        cancelled = Boolean.parseBoolean(tmp[0]);
        arrDelay = Double.parseDouble(tmp[1]);
        airportId = Double.parseDouble(tmp[2]);
        airTime = Double.parseDouble(tmp[3]);
    }

    @Override
    public String toString() {
        return this.cancelled +
                "," + this.arrDelay +
                ',' + this.airportId +
                ',' + this.airTime + '\t';
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof DataFlightsInput && (this.compareTo(o) == 0);

    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    public double getArrDelay() {
        return arrDelay;
    }

    //unused
    public double getAirportId() {
        return airportId;
    }

    //unused
    public double getAirTime() {
        return airTime;
    }

    public boolean isCancelled() {
        return cancelled;
    }
}
