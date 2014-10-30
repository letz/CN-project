import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BirdKeyWritable implements Writable {
    
    public static enum TYPES { Q1, Q2 }
    
    private String mTid = "";
    private String mDate;
    private int mWeather;
    private String mBid;
    private String type;

    public BirdKeyWritable() {
        super();
    }

    public BirdKeyWritable(String tid, String date, int weather) {
        super();
        this.setTid(tid);
        this.setWeather(weather);
        this.setDate(date);
        this.setBid(date);
        type = TYPES.Q2.toString();
    }
    
    public BirdKeyWritable(String bid) {
        super();
        this.setBid(bid);
        type = TYPES.Q1.toString();
    }

    public void readFields(DataInput in) throws IOException {
        this.setTid(in.readLine());
        this.setWeather(in.readInt());
        this.setDate(in.readLine());
        type = TYPES.Q2.toString();
        String bid = in.readLine();
        if("".equals(bid)){
            this.setBid(bid);
            type = TYPES.Q1.toString();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(getTid());
        out.writeInt(getWeather());
        out.writeBytes(getDate());
        out.writeBytes(getBid());
    }

    public String getTid() {
        return mTid;
    }

    public void setTid(String mTid) {
        this.mTid = mTid;
    }

    public String getDate() {
        return mDate;
    }

    public void setDate(String mDate) {
        this.mDate = mDate;
    }

    public int getWeather() {
        return mWeather;
    }

    public void setWeather(int mWeather) {
        this.mWeather = mWeather;
    }

    public String getBid() {
        return mBid;
    }

    public void setBid(String mBid) {
        this.mBid = mBid;
    }

    public String toString() {
        if (type.equals(TYPES.Q1)) {
            return getTid() + "," + getDate() + "," + getWeather();
        }
        return getBid();
    }

}


