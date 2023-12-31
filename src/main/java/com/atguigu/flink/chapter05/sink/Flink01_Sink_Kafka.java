package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.ArrayList;

/**
 * @Author lzc
 * @Date 2022/8/2 11:02
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        
        
        DataStreamSource<WaterSensor> stream = env.fromCollection(waterSensors);
        
        stream
            .keyBy(WaterSensor::getId)  //类：：方法名，省了new类对象的过程
            .sum("vc")
            .map(JSON::toJSONString)
            .addSink(new FlinkKafkaProducer<String>(          //.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties))
                "hadoop102:9092,hadoop103:9092",  // Kafka地址
                "s1", // 要写入的kakfa的topic
                new SimpleStringSchema()  // 序列化器 （这里是序列化过程）  继承了 DeserializationSchema<> 反序列化, SerializationSchema<>序列化
            ));
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
