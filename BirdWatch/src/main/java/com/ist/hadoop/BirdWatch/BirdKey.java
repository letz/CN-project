package com.ist.hadoop.BirdWatch;




public class BirdKey {

    public static String makeKey(String towerId, String date, int weather) {
        return "1" + towerId + "," + date + "," + weather;
    }
    
    public static String getBid(String key){
        return removeQueryIndentifier(key);
    }

    public static String removeQueryIndentifier(String key){
        //TID, DATE, WEATHER
        return new StringBuilder(key).deleteCharAt(0).toString();
    }

    public static String[] q1Keys(String key){
        //TID, DATE, WEATHER
        String raw = removeQueryIndentifier(key);
        return raw.split(",");
    }
    public static String makeKey(String birdId) {
        return "2" + birdId;
    }
    
    public static boolean isQ1(String key) {
        return key.charAt(0) == '1';
    }
    
    public static boolean isQ2(String key) {
        return key.charAt(0) == '2';
    }

}