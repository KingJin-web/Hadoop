package com.king.weblog;

import java.util.HashSet;
import java.util.Set;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-30 20:11
 */
public class KPI {
    private String remote_addr; //记录客户端的ip地址
    private String remote_user; //记录客户端用户名称,忽略属性"-"
    private String time_local;
    private String request;
    private String status;
    private String body_bytes_sent;
    private String http_referer;
    private String http_user_agent;

    private boolean valid = true; //数据是否合法

    public KPI() {

    }

    public KPI(String line) {
        String[] items = line.split(" ");
        if (items.length > 11) {
            this.remote_addr = items[0];
            this.remote_user = items[1];
            this.time_local = items[3].substring(1);
            this.request = items[6];
            this.status = items[8];
            this.body_bytes_sent = items[9];
            this.http_referer = items[10];

            String[] ss = line.split("\"");
            this.http_user_agent = ss[ss.length - 1];


            if (Integer.parseInt(status) >= 400) {
                this.valid = false;
            }
        } else {
            this.valid = false;
        }
    }

    /* 按page的pv分类 */
    public static KPI fileterPVs(String line) {
        KPI kpi = parser(line);
        //要统计的页面路径：这样只对网站中某些目录的请求进行统计，其他不计    在上线项目中，可以使用配置文件来完成
        Set<String> pages = new HashSet<>();
        pages.add("/");
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-readmap/");
        if (!pages.contains(kpi.getRequest())) {
            kpi.setValid(false);
        }
        return kpi;
    }
    public static KPI parser(String line) {

        return new KPI(line);
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    @Override
    public String toString() {
        return "KPI{" +
                "remote_addr='" + remote_addr + '\'' +
                "\nremote_user='" + remote_user + '\'' +
                "\ntime_local='" + time_local + '\'' +
                "\nrequest='" + request + '\'' +
                "\nstatus='" + status + '\'' +
                "\nbody_bytes_sent='" + body_bytes_sent + '\'' +
                "\nhttp_referer='" + http_referer + '\'' +
                "\nhttp_user_agent='" + http_user_agent + '\'' +
                "\nvalid=" + valid +
                '}';
    }

    public static void main(String[] args) {
        KPI kpi = new KPI("114.112.141.6 - - [04/Jan/2012:00:00:02 +0800] \"GET /popwin_js.php?fid=133 HTTP/1.1\" 200 11 \"http://www.itpub.net/thread-1554759-4-10.html\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; InfoPath.3; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)");
        System.out.println(kpi);
    }
}
