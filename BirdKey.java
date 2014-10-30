

public class BirdKey {

    public static String makeKey(String towerId, String date, int weather) {
        return "1" + towerId + "," + date + "," + weather;
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