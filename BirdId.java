import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BirdId extends BirdDataWritable {

    private String mId;

    public BirdId() {
        this("");
    }

    BirdId(String id) {
        super();
        this.setId(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.setId(in.readLine());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(getId());
    }

    public String toString() {
        return getId();

    }

    public String getId() {
        return mId;
    }

    public void setId(String mId) {
        this.mId = mId;
    }

}

