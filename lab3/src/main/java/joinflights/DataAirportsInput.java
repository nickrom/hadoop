package joinflights;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by user on 17.12.16.
 */
public class DataAirportsInput implements Writable {

    private Integer code;
    private String name;

    public Integer getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public DataAirportsInput() {}

    public DataAirportsInput(String str) {
        String[] tmp = str.split(",");
        if(!tmp[0].equals("Code")) {
            this.code = Integer.parseInt(tmp[0].substring(1, tmp[0].length() - 1));
            this.name = tmp[1] + '\"';
        }
    }

    @Override
    public String toString() {
        return this.code + "," + this.name;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(this.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String tmp[] = dataInput.readLine().split(",");
        code = Integer.parseInt(tmp[0]);
        name = tmp[1];
    }
}
