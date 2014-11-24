package com.ist.hadoop.BirdWatch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class BirdDataWritable implements Writable {

    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    private int mWeight;
    private int mWingSpan;
    private String mTower = "T-0000";
    private String mDate = new Date(0).toString();

    public BirdDataWritable() {
        super();
        setWeight(0);
        setWingSpan(0);
    }

    public BirdDataWritable(int value) {
        super();
        this.setWeight(value);

    }
    public BirdDataWritable(String tid, int ws) {
        super();
        this.setTower(tid);
        this.setWingSpan(ws);


    }
    public BirdDataWritable(String date) {
        super();
        this.setDate(date);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.setWingSpan(in.readInt());
        this.setWeight(in.readInt());
        String[] inData = in.readLine().split(",");
        this.setTower(inData[0]);
        this.setDate(inData[1]);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(getWingSpan());
        out.writeInt(getWeight());
        out.writeBytes(getTower()+","+getDate());
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

    public String getDate() {
        return mDate;
    }

    public Date getRealDate() {
        try {
            return formatter.parse(mDate);
        } catch (ParseException e) {
            return new Date(0);
        } catch (NullPointerException e) {
            return new Date(0);
        }
    }

    public void setDate(String data) {
        this.mDate = data;
    }

    public String getTower() {
        return mTower;
    }

    public void setTower(String mTower) {
        this.mTower = mTower;
    }


}

