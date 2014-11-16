package com.ist.hadoop.BirdWatch;

public class Log {

    private String [] mLog;

    public Log(String raw) {
        mLog = raw.replaceAll("\\s+","").split(",");
       // sanitizeAttributes();
    }
    public String getTowerId() {
        return mLog[0];
    }

    public String getDate() {
        return mLog[1];
    }

    public String getTime() {
        return mLog[2];
    }

    public String getBirdId() {
        return mLog[3];
    }

    public int getWeight() {
        return Integer.parseInt(mLog[4]);
    }

    public int getWingSpan() {
        return Integer.parseInt(mLog[5]);
    }

    public int getWeather() {
        return Integer.parseInt(mLog[6]);
    }
}