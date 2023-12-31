package com.atguigu.flink.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author lzc
 * @Date 2022/8/2 9:25
 */
public class Flink02_Connect {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        DataStreamSource<Integer> s1 = env.fromElements(10, 11, 9, 20, 12);
        DataStreamSource<String> s2 = env.fromElements("a", "c", "b");
        
        ConnectedStreams<Integer, String> s12 = s1.connect(s2);  //两个流合在一起，但是内部还是分开的，相当于一个并行度处理两个流，流之间相互独立，类型可以不一样，只能连个流
        
        s12
            .map(new CoMapFunction<Integer, String, String>() {     //输入两个流的类型，及合并后的输出类型
                @Override
                public String map1(Integer value) throws Exception {
                    return value + "<";
                }
                
                @Override
                public String map2(String value) throws Exception {
                    return value + ">";
                }
            })
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
connect:
    1. 只能两个流连在一起
    2. 两个流的数据类型可以不一样, 实际情况也是大部分情况都是不同类型
 */