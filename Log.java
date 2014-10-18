public class Log {

    private String [] mLog;

    public Log(String raw) {
        mLog = raw.split(",");
        sanitizeAttributes();
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

    public String getWeight() {
        return mLog[4];
    }

    public String getWingSpan() {
        return mLog[5];
    }

    public String getWeather() {
        return mLog[6];
    }

    private void sanitizeAttributes() {
        for(String s : mLog) {
            s = s.trim();
        }
    }

}
