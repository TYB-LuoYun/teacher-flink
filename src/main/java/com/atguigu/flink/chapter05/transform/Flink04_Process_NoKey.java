package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/8/2 9:25
 */
public class Flink04_Process_NoKey {        //没有keyby的process方法
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        // KeyBy之前用
        // 计算所有传感器的水位和
        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("sensor_1", 1L, 10),
            new WaterSensor("sensor_2", 1L, 20),
            new WaterSensor("sensor_1", 1L, 30),
            new WaterSensor("sensor_1", 1L, 40),
            new WaterSensor("sensor_2", 1L, 50),
            new WaterSensor("sensor_2", 1L, 50)
        );
    
        stream
            .process(new ProcessFunction<WaterSensor, String>() {  //进入process算子前是多少并行度，则方法内部的局部变量就会出现几个。计算后是按各自并行度内的数据相加，与key无关
                // 并行度是: 有几个sum?  2个
                int sum = 0;
            
                @Override
                public void processElement(WaterSensor value,  //  需要处理的元素
                                           Context ctx,  // 上下文
                                           Collector<String> out) throws Exception {
                    sum += value.getVc();
                
                    out.collect("总的水位和: " + sum);       //收集处理后的结果，来一个数据，处理得出一个数据
                }
            })
            .print();       //将每一次处理完的数据打印
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
 
 */