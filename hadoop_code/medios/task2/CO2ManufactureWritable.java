package hadoop_code.medios.task2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CO2ManufactureWritable implements Writable {
    private int count;
    private double co2;

    public CO2ManufactureWritable() {
    }

    public CO2ManufactureWritable(double co2, int count) {
        this.co2 = co2;
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getCo2() {
        return co2;
    }

    public void setCo2(double co2) {
        this.co2 = co2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeDouble(co2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        co2 = in.readDouble();
    }
}