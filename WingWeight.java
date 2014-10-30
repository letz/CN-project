import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WingWeight extends BirdDataWritable {
    private int mWeight;
    private int mWingSpan;

    public WingWeight() {
        this(0, 0);
    }

    public WingWeight(int weight, int wingspan) {
        super();
        this.setWeight(weight);
        this.setWingSpan(wingspan);

    }

    public void readFields(DataInput in) throws IOException {
        this.setWingSpan(in.readInt());
        this.setWeight(in.readInt());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(getWingSpan());
        out.writeInt(getWeight());
    }

    public int getWeight() {
        return mWeight;
    }

    public void setWeight(int mWeightSum) {
        this.mWeight = mWeightSum;
    }

    public int getWingSpan() {
        return mWingSpan;
    }

    public void setWingSpan(int mWingSpan) {
        this.mWingSpan = mWingSpan;
    }

}

