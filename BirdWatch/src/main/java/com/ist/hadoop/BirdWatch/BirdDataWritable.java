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
    private String mDate = "";

    public BirdDataWritable() {
        super();
    }

    public BirdDataWritable(int value, String query) {
        super();
        if (query.equals("Q1")){
            this.setWingSpan(value);
        }
        else{
            this.setWeight(value);
        }

    }

    public BirdDataWritable(String date) {
        super();
        this.setDate(date);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.setWingSpan(in.readInt());
        this.setWeight(in.readInt());
        this.setDate(in.readLine());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(getWingSpan());
        out.writeInt(getWeight());
        out.writeBytes(getDate());
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
        }
    }

    public void setDate(String data) {
        this.mDate = data;
    }

}

