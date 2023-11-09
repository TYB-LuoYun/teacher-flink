package com.atguigu.flink.chapter05.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2020/12/8 22:44
 */
public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
          .readTextFile("input")  //从文本读取

          .print();

        env.execute();
    }
}
