package com.ist.hadoop.BirdWatch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class BirdStatsWritable implements Writable {

    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    private int mWeightSum;
    private int mMaxWingSpan;
    private Date mDate = null;

    public BirdStatsWritable() {
        super();
    }

    public BirdStatsWritable(Date date) {
        super();
        this.setDate(date);
    }

    public BirdStatsWritable(int value, String query) {
        super();
        if (query.equals("Q1")){
            this.setMaxWingSpan(value);
        }
        else{
            this.setWeightSum(value);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.setWeightSum(in.readInt());
        this.setMaxWingSpan(in.readInt());
        this.setDate(in.readLine());

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(getWeighSum());
        out.writeInt(getMaxWingSpan());
        out.writeBytes(formatter.format(getDate()));
    }

    public int getMaxWingSpan() {
        return mMaxWingSpan;
    }

    public void setMaxWingSpan(int mMaxWingSpan) {
        this.mMaxWingSpan = mMaxWingSpan;
    }

    public int getWeighSum() {
        return mWeightSum;
    }

    public void setWeightSum(int mWeightedSum) {
        this.mWeightSum = mWeightedSum;
    }

    @Override
    public String toString() {
        if(mDate == null && mWeightSum == 0)
            return this.getMaxWingSpan() + "";
        if(mDate == null && mMaxWingSpan == 0)
            return this.getWeighSum() + "";
        else
            return formatter.format(this.getDate());

    }

    public Date getDate() {
        return mDate;
    }

    public void setDate(Date date) {
        this.mDate = date;
    }

    public void setDate(String date) {
        try {
            this.setDate(formatter.parse(date));
        } catch (ParseException e) {
            this.setDate(new Date(0));
        }
    }

}

