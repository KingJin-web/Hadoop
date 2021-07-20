package com.king.zookeeper.master2;

import lombok.Data;

/**
 * @author Exception
 * @create 2021-07-16-11:48
 * @content 服务器基本信息
 */
@Data
public class ServerData {
    private String address;
    private Integer id;
    private String name;
}
