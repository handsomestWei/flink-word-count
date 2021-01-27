package com.wjy.flink.wc.func;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * key处理函数：统计key重复出现频率</br>
 * 每个key都保存有一个状态
 */
public class RepeatDuration extends KeyedProcessFunction<String, Tuple1<String>, Tuple2<String, Long>> {

    private static final long serialVersionUID = -5371011323986054046L;
    private transient ValueState<Long> repeatDuration;

    @Override
    public void open(Configuration conf) {
        // 初始化键的状态
        repeatDuration = getRuntimeContext()
                .getState(new ValueStateDescriptor<Long>("repeatDuration", Long.class));
    }

    @Override
    public void processElement(Tuple1<String> value,
            KeyedProcessFunction<String, Tuple1<String>, Tuple2<String, Long>>.Context ctx,
            Collector<Tuple2<String, Long>> out) throws Exception {
        Long cTime = System.currentTimeMillis();
        // 取出上一次键的状态值
        Long sTime = repeatDuration.value();
        if (sTime != null) {
            out.collect(Tuple2.of(value.f0, (cTime - sTime) / 1000));
            // 清除键的状态
            // startTime.clear();
        }
        // 更新键的状态
        repeatDuration.update(cTime);
    }

}
