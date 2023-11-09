package com.atguigu.flink.chapter05.mode;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/8/2 15:23
 */
public class Flink01_RuntimeMode {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);  //StreamExecutionEnvironment 是流模式  ExecutionEnvironment获取的是批模式
        env.setParallelism(1);
        
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC); //可自动识别流和批，只要有数据是流数据则按流模式处理

        env
//            .readTextFile("input/words.txt")  //读取文本  有界流
            .socketTextStream("hadoop102", 9999)    //读取socket文件 ，无界流
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                //map,faltmap,filter等等 这些算子需要传入指定算子的 名称+Function 类的对象（可匿名内部类，lambda表达式（如果是元祖类型需要注意泛型擦除情况，要显示指定泛型类型），或者定义内部类传入）
                @Override
                public void flatMap(String line,
                                    Collector<Tuple2<String, Long>> out) throws Exception {
                    for (String word : line.split(" ")) {

                        out.collect(Tuple2.of(word, 1L));
                    }

                }
            })
            .keyBy(t -> t.f0)
            .sum(1)
            .print();
            
        
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
如果是有界的source: 可以使用流模式也可以使用批模式
如果是无界的source: 只能使用流模式
 */