package hadoop_code.dificil.task10;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CarData implements Writable {
    private String manufacturer;
    private int year;
    private double co2Emissions;

    public CarData() {
    }

    public CarData(String manufacturer, int year, double co2Emissions) {
        this.manufacturer = manufacturer;
        this.year = year;
        this.co2Emissions = co2Emissions;
    }

    public String getManufacturer() {
        return manufacturer;
    }

    public void setManufacturer(String manufacturer) {
        this.manufacturer = manufacturer;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public double getCo2Emissions() {
        return co2Emissions;
    }

    public void setCo2Emissions(double co2Emissions) {
        this.co2Emissions = co2Emissions;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(manufacturer);
        out.writeInt(year);
        out.writeDouble(co2Emissions);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        manufacturer = in.readUTF();
        year = in.readInt();
        co2Emissions = in.readDouble();
    }
}