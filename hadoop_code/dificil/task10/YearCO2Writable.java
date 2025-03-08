package hadoop_code.dificil.task10;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearCO2Writable implements Writable {
    private int year;
    private double avgCO2;

    public YearCO2Writable() {
    }

    public YearCO2Writable(int year, double avgCO2) {
        this.year = year;
        this.avgCO2 = avgCO2;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public double getAvgCO2() {
        return avgCO2;
    }

    public void setAvgCO2(double avgCO2) {
        this.avgCO2 = avgCO2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeDouble(avgCO2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        avgCO2 = in.readDouble();
    }

    @Override
    public String toString() {
        return year + "_" + avgCO2;
    }
}