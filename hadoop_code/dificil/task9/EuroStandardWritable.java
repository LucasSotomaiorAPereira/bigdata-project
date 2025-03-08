package hadoop_code.dificil.task9;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EuroStandardWritable implements Writable {

    private String k;
    private int v;

    public EuroStandardWritable() {}

    public EuroStandardWritable(String k, int v) {
        this.k = k;
        this.v = v;
    }

    public String getK() {
        return k;
    }

    public void setK(String k) {
        this.k = k;
    }

    public int getV() {
        return v;
    }

    public void setV(int v) {
        this.v = v;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(k);
        dataOutput.writeInt(v);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        k = dataInput.readUTF();
        v = dataInput.readInt();
    }

    @Override
    public String toString() {
        return k + ", " + v;
    }

}
