package hadoop_code.dificil.task10;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ManufacturerYearKey implements WritableComparable<ManufacturerYearKey> {
    private Text manufacturer;
    private int year;

    public ManufacturerYearKey() {
        this.manufacturer = new Text();
    }

    public ManufacturerYearKey(String manufacturer, int year) {
        this.manufacturer = new Text(manufacturer);
        this.year = year;
    }

    public Text getManufacturer() {
        return manufacturer;
    }

    public void setManufacturer(Text manufacturer) {
        this.manufacturer = manufacturer;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        manufacturer.write(out);
        out.writeInt(year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        manufacturer.readFields(in);
        year = in.readInt();
    }

    @Override
    public int compareTo(ManufacturerYearKey other) {
        int cmp = this.manufacturer.compareTo(other.manufacturer);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(this.year, other.year);
    }
}