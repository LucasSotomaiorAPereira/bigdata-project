package hadoop_code.medios.task1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ModelKmWritable implements Writable {
    private String model;
    private double km;

    public ModelKmWritable() {
    }

    public ModelKmWritable(String model, double km) {
        this.model = model;
        this.km = km;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public double getKm() {
        return km;
    }

    public void setKm(double km) {
        this.km = km;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(model);
        out.writeDouble(km);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        model = in.readUTF();
        km = in.readDouble();
    }
}