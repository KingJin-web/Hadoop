package com.king.zookeeper.master2;

import lombok.Data;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;

import java.io.IOException;

/**
 * @author Exception
 * @create 2021-07-16-11:45
 * @content 微服务的配置信息,要存到zk中.
 */
@Data
public class ServerConfig implements Record {
    private String dbUrl;
    private String dbPwd;
    private String dbUser;

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this,tag);
        archive.writeString(dbUrl,"dbUrl");
        archive.writeString(dbPwd,"dbPwd");
        archive.writeString(dbUser,"dbUser");
        archive.endRecord(this,tag);
    }

    @Override
    public void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord(tag);
        this.dbUrl = archive.readString("dbUrl");
        this.dbPwd = archive.readString("dbPwd");
        this.dbUser = archive.readString("dbUser");
        archive.endRecord(tag);
    }
}
