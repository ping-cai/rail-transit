package edu.sicau.ping.show.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TimeUtil {
    public static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static List<String> getTimeInterval(int granularity) {
        Timestamp startTime = Timestamp.valueOf("2022-04-12 06:00:00");
        Timestamp endTime = Timestamp.valueOf("2022-04-12 23:30:00");
        int addTime = granularity * 1000 * 60;
        long times = (endTime.getTime() - startTime.getTime()) / addTime;
        ArrayList<String> res = new ArrayList<>();
        while (times >= 0) {
            LocalDateTime localDateTime = startTime.toLocalDateTime();
            res.add(localDateTime.format(timeFormatter));
            startTime.setTime(startTime.getTime() + addTime);
            times--;
        }
        return res;
    }

    public static void main(String[] args) {
        System.out.println(getTimeInterval(15));
    }
}
