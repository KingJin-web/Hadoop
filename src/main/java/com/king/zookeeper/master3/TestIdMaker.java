package com.king.zookeeper.master3;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-16 16:17
 */
public class TestIdMaker {
    public static void main(String[] args) throws Exception {
        IdMaker idMaker = new IdMaker("NameService/IdGen", "ID");
        idMaker.start();
        try {
            for (int i = 0; i < 10111; i++) {
                String id = idMaker.generateId(IdMaker.RemoveMethod.NONE);
                System.out.println(id);
            }
        } finally {
            idMaker.stop();

        }
    }

}
