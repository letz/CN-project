package com.ist.hadoop.BirdWatch;




public class BirdKey {


    public static String makeKey(String towerId, String date, String query) {
        return query + towerId + "," + date;
    }

    public static String makeKey(String value, String query) {
        return query + value;
    }

    public static String getBid(String key){
        return removeQueryIndentifier(key);
    }

    public static String removeQueryIndentifier(String key){
        //TID, DATE, WEATHER
        return new StringBuilder(key).deleteCharAt(0).toString();
    }

    public static String[] q1Keys(String key){
        //TID, DATE
        String raw = removeQueryIndentifier(key);
        return raw.split(",");
    }

    public static String q3Key(String key){
        //BID
        return removeQueryIndentifier(key);
    }

    public static boolean isQ1(String key) {
        return key.charAt(0) == '1';
    }

    public static boolean isQ2(String key) {
        return key.charAt(0) == '2';
    }

    public static boolean isQ3(String key) {
        return key.charAt(0) == '3';
    }
}