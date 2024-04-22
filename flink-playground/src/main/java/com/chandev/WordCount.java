package com.chandev;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream< String > text = env.readTextFile(params.get("input"));

        DataStream < Tuple2 < String, Integer >> counts =
                text.filter(value -> value.startsWith("N"))
                    .map(new Tokenizer()) // split up the lines in pairs (2-tuples) containing: tuple2 {(name,1)...}
                    .keyBy(t -> t.f0)
                    .sum(1); // group by the tuple field "0" and sum up tuple field "1"

        Iterator<Tuple2<String, Integer>> iterator = counts.collectAsync();
        // execute program
        env.execute("Streaming WordCount");
        while (iterator.hasNext()) {
            System.out.println("test: " + iterator.next());
        }

    }

    public static final class Tokenizer implements MapFunction < String, Tuple2 < String, Integer >> {
        public Tuple2 < String, Integer > map(String value) {
            return new Tuple2<> (value, 1);
        }
    }
}
