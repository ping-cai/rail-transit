package edu.sicau.ping.show.util;

import java.util.Random;

public class ColorUtils {
    public static String generateRandomColor() {
        //生成随机对象
        Random random = new Random();
        //生成红色颜色代码
        String red = Integer.toHexString(random.nextInt(256)).toUpperCase();
        //生成绿色颜色代码
        String green = Integer.toHexString(random.nextInt(256)).toUpperCase();
        //生成蓝色颜色代码
        String blue = Integer.toHexString(random.nextInt(256)).toUpperCase();

        //判断红色代码的位数
        red = red.length() == 1 ? "0" + red : red;
        //判断绿色代码的位数
        green = green.length() == 1 ? "0" + green : green;
        //判断蓝色代码的位数
        blue = blue.length() == 1 ? "0" + blue : blue;
        //生成十六进制颜色值
        return "#" + red + green + blue;
    }
}
