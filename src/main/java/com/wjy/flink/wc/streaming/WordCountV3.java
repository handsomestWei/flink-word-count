package com.wjy.flink.wc.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;

import com.wjy.flink.wc.func.RepeatDuration;

/**
 * 使用Keyed State统计单词重复出现频率
 */
public class WordCountV3 {

    // unix环境启动netcat 执行 nc -lk 9000
    // windows环境安装 https://nmap.org/ncat/ 执行ncat -lk 9000
    @SuppressWarnings({ "serial", "rawtypes", "unchecked" })
    public static void main(String[] args) throws Exception {

        ParameterTool paramFromProps = null;
        String hostName = "localhost";
        int port = 9000;

        // 获取外部参数 --configPath /xxx/xx/config.properties
        ParameterTool parameters = ParameterTool.fromArgs(args);

        // 读取参数：配置文件目录configPath
        String configPath = parameters.get("configPath", null);
        if (configPath != null) {
            // 读取配置文件
            paramFromProps = ParameterTool.fromPropertiesFile(configPath);
            hostName = paramFromProps.get("hostName", hostName);
            port = paramFromProps.getInt("port", port);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 把配置参数存入上下文
        if (paramFromProps != null) {
            env.getConfig().setGlobalJobParameters(paramFromProps);
        }

        // 设定时间特征为处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 数据源为socket流。监听9000端口，每行输入作为一条记录，转换为DataStream
        DataStreamSource<String> textDs = env.socketTextStream(hostName, port, "\n");
        
        // 过滤
        DataStream<String> ds0 = textDs.filter(new FilterFunction<String>() {
            
            @Override
            public boolean filter(String value) throws Exception {
                // 去空
                if (value == null || value.trim().isEmpty()) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        
        // 转换为一元组，元素是：String型单词
        DataStream<Tuple1<String>> ds = ds0.flatMap(new FlatMapFunction<String, Tuple1<String>>() {

            @Override
            public void flatMap(String value, Collector<Tuple1<String>> out) throws Exception {
                // 按空格拆分单词
                for (String word : value.split("\\s")) {
                    out.collect(Tuple1.of(word));
                }
            }
        });

        // 自定义key选择器
        ds.keyBy(new KeySelector<Tuple1<String>, String>() {

            @Override
            public String getKey(Tuple1<String> value) throws Exception {
                // 返回元组的第0列单词
                return value.getField(0);
            }

        })
                .process(new RepeatDuration()) // 使用key处理函数
                .addSink(new PrintSinkFunction());

        // 提交
        env.execute("WordCount Repeat Duration");
    }

}
