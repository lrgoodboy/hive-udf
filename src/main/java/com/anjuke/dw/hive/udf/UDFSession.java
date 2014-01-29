package com.anjuke.dw.hive.udf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFSession extends UDF {

    private DateFormat dfSec = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private DateFormat dfMilli = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

    private int sessionId = 1;
    private String lastKey;
    private Date lastTime;

    public int evaluate(String key, String time, int minutes) {

        Date date = parseDate(time);

        if ((lastKey == null && key != null)
                || (lastKey != null && key == null)
                || (lastKey != null && key != null && !lastKey.equalsIgnoreCase(key))) {

            lastKey = key;
            lastTime = date;
            sessionId = 1;
            return sessionId;
        }

        if ((lastTime == null && date != null)
                || (lastTime != null && date == null)
                || (lastTime != null && date != null
                        && date.getTime() - lastTime.getTime() > minutes * 60 * 1000)) {

            ++sessionId;
        }

        lastTime = date;
        return sessionId;
    }

    private Date parseDate(String source) {

        if (source == null || source.isEmpty()) {
            return null;
        }

        try {
            return dfMilli.parse(source);
        } catch (ParseException e) {}

        try {
            return dfSec.parse(source);
        } catch (ParseException e) {}

        return null;
    }

}
