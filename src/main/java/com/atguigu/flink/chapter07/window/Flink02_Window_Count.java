package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/3 14:18
 */
public class Flink02_Window_Count {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        env
            .socketTextStream("hadoop102", 8888)
            .map(line -> {
                String[] data = line.split(",");
            
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2])
                );
            })
            .keyBy(WaterSensor::getId)
            // 定义长度为3的基于个数的滚动窗口
//            .countWindow(3)
            // / 定义长度为3(窗口内元素的最大个数), 滑动步长为2的的基于个数的滑动窗口
            .countWindow(3, 2)
            .process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<WaterSensor> elements,
                                    Collector<String> out) throws Exception {
    
                    List<WaterSensor> list = AtguiguUtil.toList(elements);
                    out.collect(" key:" + key + "  " + list);
        
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
