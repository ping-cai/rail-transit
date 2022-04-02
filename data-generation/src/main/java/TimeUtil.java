import java.sql.Date;
import java.sql.Timestamp;
import java.util.Random;

/**
 * 时间工具
 */
public class TimeUtil {
    private static final Random random = new Random();
    public static final int HOUR = 1000 * 60 * 60;

    public static Date getRandomDate() {
        Date date = new Date(System.currentTimeMillis());
        long time = date.getTime();
        long nextLong = random.nextInt(2 * HOUR) - HOUR;
        date.setTime(time + nextLong);
        return date;
    }

    public static Timestamp getRandomTimestamp() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        long time = timestamp.getTime();
        long nextLong = random.nextInt(2 * HOUR) - HOUR;
        timestamp.setTime(time + nextLong);
        return timestamp;
    }
}
