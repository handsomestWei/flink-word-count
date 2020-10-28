package com.wjy.flink.wc.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

/**
 * 自定义的标准控制台输出
 */
@PublicEvolving
public class StdPrintSink<IN> extends PrintSinkFunction<IN> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    @Override
    public void invoke(IN record) {
        super.invoke((IN) ("word count print>" + record.toString()));
    }

}
