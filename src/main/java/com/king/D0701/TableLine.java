package com.king.D0701;

import com.king.util.TimeUtil;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-01 20:00
 */
public class TableLine {
    private String imsi;
    private String imei;
    private String position;
    private String time;
    private String timeFlag;
    private long Time;
    private Date day;
    private String url;
    private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public TableLine() {
    }

    public TableLine(String l) {
        String[] arr = l.split("\t");

        String time = arr[4];

        this.imsi = arr[0];
        this.Time = TimeUtil.parse(time);

    }

    public void set(String lien, boolean source, String date, String[] timepoint) throws LineException {
        String[] lineSplit = lien.split("\t");
        if (source) {
            this.imsi = lineSplit[0];
            this.imei = lineSplit[1];
            this.position = lineSplit[3];
            this.time = lineSplit[4];
        } else {
            this.imsi = lineSplit[0];
            this.imei = lineSplit[1];
            this.position = lineSplit[2];
            this.time = lineSplit[3];
            this.url = lineSplit[4];
        }
        //检查日期合法性
        if (!this.time.startsWith(date)) {
            throw new LineException("", -1);
        }
        try {
            this.day = this.formatter.parse(this.time);
        } catch (ParseException e) {
            throw new LineException("时间格式有误", 0);
        }

        //计算所属时间段
        int i = 0;
        int n = timepoint.length;
        int hour = Integer.parseInt(time.split(" ")[1].split(":")[0]);
        while (i < n && Integer.parseInt(timepoint[i]) <= hour) {
            i++;
        }

        if (i < n) {
            if (i == 0) {
                this.timeFlag = ("00-" + timepoint[i]);
            } else {
                this.timeFlag = (timepoint[i - 1] + "-" + timepoint[i]);
            }
        } else {
            throw new LineException("", -1);
        }
    }


    public Text OutKey() {
        return new Text(this.imsi + "|" + this.timeFlag);
    }

    public Text OutValue() {
        this.Time = (day.getTime() / 1000L);
        return new Text(this.position + "|" + this.Time);
    }

    public static void main(String[] args) throws LineException {
        TableLine p = new TableLine();
        p.set("0000000000\t0054775807\t3\t00000033\t2021-07-01 08:35:43", true, "2021-07-01", new String[]{"07", "17", "24"});
        System.out.println(p);
    }

    @Override
    public String toString() {
        return "TableLine{" +
                "imsi='" + imsi + '\'' +
                ", position='" + position + '\'' +
                ", time='" + time + '\'' +
                ", timeFlag='" + timeFlag + '\'' +
                ", Time=" + Time +
                ", day=" + day +
                ", formatter=" + formatter +
                '}';
    }

    public String getPosition() {
        return position;
    }

    public String getImsi() {
        return imsi;
    }

    public String getImei() {
        return imei;
    }

    public String getUrl() {
        return url;
    }
}
