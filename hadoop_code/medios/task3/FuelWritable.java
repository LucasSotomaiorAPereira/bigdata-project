package hadoop_code.medios.task3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FuelWritable implements Writable {

    private String type;
    private double price6000miles;
    private double price12000miles;

    public FuelWritable() {}

    public FuelWritable(String type, double price6000miles, double price12000miles) {
        this.type = type;
        this.price6000miles = price6000miles;
        this.price12000miles = price12000miles;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public double getPrice6000miles() {
        return price6000miles;
    }

    public void setPrice6000miles(double price6000miles) {
        this.price6000miles = price6000miles;
    }

    public double getPrice12000miles() {
        return price12000miles;
    }

    public void setPrice12000miles(double price12000miles) {
        this.price12000miles = price12000miles;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type);
        dataOutput.writeDouble(price6000miles);
        dataOutput.writeDouble(price12000miles);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        type = dataInput.readUTF();
        price6000miles = dataInput.readDouble();
        price12000miles = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "type=" + type + ", 12000=" + price12000miles + ", 6000=" + price6000miles;
    }
}
