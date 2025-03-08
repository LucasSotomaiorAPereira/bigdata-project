package hadoop_code.medios.task4;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VehicleTaxWritable implements Writable {

    private double default6Months;
    private double default12Months;
    private double first6Months;
    private double first12Months;

    public VehicleTaxWritable() {}

    public VehicleTaxWritable(double default6Months, double default12Months, double first6Months, double first12Months) {
        this.default6Months = default6Months;
        this.default12Months = default12Months;
        this.first6Months = first6Months;
        this.first12Months = first12Months;
    }

    public double getDefault6Months() {
        return default6Months;
    }

    public void setDefault6Months(double default6Months) {
        this.default6Months = default6Months;
    }

    public double getDefault12Months() {
        return default12Months;
    }

    public void setDefault12Months(double default12Months) {
        this.default12Months = default12Months;
    }

    public double getFirst6Months() {
        return first6Months;
    }

    public void setFirst6Months(double first6Months) {
        this.first6Months = first6Months;
    }

    public double getFirst12Months() {
        return first12Months;
    }

    public void setFirst12Months(double first12Months) {
        this.first12Months = first12Months;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(default6Months);
        dataOutput.writeDouble(default12Months);
        dataOutput.writeDouble(first6Months);
        dataOutput.writeDouble(first12Months);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        default6Months = dataInput.readDouble();
        default12Months = dataInput.readDouble();
        first6Months = dataInput.readDouble();
        first12Months = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "default6Months=" + default6Months + ", default12Months=" + default12Months +
                ", first6Months=" + first6Months + ", first12Months=" + first12Months;
    }
}
