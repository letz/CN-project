public class Log {

    private String [] mLog;

    public Log(String raw) {
        raw = raw.replace(raw.charAt(raw.length()-1), '\n');
        mLog = raw.replaceAll(raw.charAt(8)+"", "").split(",");
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
