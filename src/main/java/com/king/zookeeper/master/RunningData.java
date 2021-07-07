package com.king.zookeeper.master;

import org.apache.jute.*;
import org.apache.zookeeper.server.ByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-06 20:02
 */
public class RunningData implements Record {
    //服务器编号
    private Long cid;
    private String name;

    public Long getCid() {
        return cid;
    }

    public void setCid(Long cid) {
        this.cid = cid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        archive.writeLong(cid, "cid");
        archive.writeString(name, "name");
        archive.endRecord(this, tag);
    }

    @Override
    public void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord(tag);
        this.cid = archive.readLong("cid");
        this.name = archive.readString("name");
        archive.endRecord(tag);
    }


    public static void main(String[] args) throws IOException {
        RunningData rd = new RunningData();
        rd.name = "Hello";
        rd.cid = 1L;

        //序列化上面对象
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa =BinaryOutputArchive.getArchive(baos);
        rd.serialize(boa,"header");
        byte[] bytes = baos.toByteArray();

        //反序列化 bs 数组
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
        BinaryInputArchive bia = BinaryInputArchive.getArchive(bbis);
        RunningData header  = new RunningData();
        header.deserialize(bia,"cerate");

        System.out.println(header);


    }

    @Override
    public String toString() {
        return "RunningData{" +
                "cid=" + cid +
                ", name='" + name + '\'' +
                '}';
    }
}
