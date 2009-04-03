package bixo.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeStampUtil {

    public static String nowWithUnderLine() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy_MM_dd__HH_mm_ss");
        return format.format(new Date(System.currentTimeMillis()));
    }

}
