package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import org.example.SpeedCalculator;

public class StockABC {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Start time for performance measurement
        long startTime = System.currentTimeMillis();

        // Load CSV file and convert to DataStream
        DataStream<Tuple3<String, Double, String>> input = env.addSource(new SourceFunction<Tuple3<String, Double, String>>() {
            @Override
            public void run(SourceContext<Tuple3<String, Double, String>> ctx) throws Exception {
                String filePath = "./data/goog_msft.csv"; // Replace with the actual CSV file path
                BufferedReader reader = new BufferedReader(new FileReader(filePath));
                String line;
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                while ((line = reader.readLine()) != null) {
                    try {
                        String[] fields = line.split(",");
                            ctx.collect(new Tuple3<>(
                                    fields[0] + " 18:00:00",
                                    Double.parseDouble(fields[5]),
                                    fields[7]
                            ));

                    } catch (Exception e) {
                        System.err.printf("Skipping invalid data line: %s%n", line);
                    }
                }
                reader.close();
            }

            @Override
            public void cancel() {
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Double, String>>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                            return LocalDateTime.parse(event.f0, formatter).toInstant(ZoneOffset.UTC).toEpochMilli();
                        })
        );

        // 定义CEP模式
        Pattern<Tuple3<String, Double, String>, ?> crimePattern = Pattern
        .<Tuple3<String, Double, String>>begin("A")
        .where(new IterativeCondition<Tuple3<String, Double, String>>() {
            @Override
            public boolean filter(Tuple3<String, Double, String> event, Context<Tuple3<String, Double, String>> ctx) {
                return "MSFT".equals(event.f2);
            }
        })
        .followedByAny("B")
        .timesOrMore(10)
        .allowCombinations()
        .where(new IterativeCondition<Tuple3<String, Double, String>>() {
            @Override
            public boolean filter(Tuple3<String, Double, String> event, Context<Tuple3<String, Double, String>> ctx) throws Exception {
                Tuple3<String, Double, String> previousEvent = null;
                for (Tuple3<String, Double, String> e : ctx.getEventsForPattern("B")) {
                    previousEvent = e; // 获取最后一个 Z1
                }
                if (previousEvent == null) return true;

                return event.f1 >= previousEvent.f1 && "GOOG".equals(event.f2);
            }
        })
        .followedByAny("C")
        .where(new IterativeCondition<Tuple3<String, Double, String>>() {
            @Override
            public boolean filter(Tuple3<String, Double, String> event, Context<Tuple3<String, Double, String>> ctx) throws Exception {
                Tuple3<String, Double, String> r =  new Tuple3<>();
                for (Tuple3<String, Double, String> e : ctx.getEventsForPattern("A")) {
                    r = e;
                    break;
                }

                return "MSFT".equals(event.f2) && event.f1 >= 1.2 * r.f1;
            }
        })
        .within(Time.days(40));

        // Apply the pattern
        PatternStream<Tuple3<String, Double, String>> patternStream = CEP.pattern(input, crimePattern);

        // Initialize counters
        final long[] eventCount = {10000};
        final long[] matchCount = {0};
        final long[] maxLatency = {0};
        final long[] minLatency = {Long.MAX_VALUE};
        final long[] totalLatency = {0};

        // Select and process matched patterns
        DataStream<String> resultStream = patternStream.select(
                (PatternSelectFunction<Tuple3<String, Double, String>, String>) pattern -> {
                    Tuple3<String, Double, String> A = pattern.get("A").get(0);
                    Tuple3<String, Double, String> C = pattern.get("C").get(0);

                    // Get Z1 and Z2 events
                    List<Tuple3<String, Double, String>> B = pattern.get("B");

                    // Extract IDs from Z1 and Z2
                    String Bdates = B.stream()
                            .map(event -> event.f0) // Extract id
                            .collect(Collectors.joining(", "));

                    // Calculate latency
                    long eventTime = LocalDateTime.parse(A.f0, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                            .toInstant(ZoneOffset.UTC).toEpochMilli();
                    long latency = System.currentTimeMillis() - eventTime;

                    synchronized (maxLatency) {
                        maxLatency[0] = Math.max(maxLatency[0], latency);
                        minLatency[0] = Math.min(minLatency[0], latency);
                        totalLatency[0] += latency;
                        matchCount[0]++;
                    }

                    // Return formatted result string
                    return String.format("a_date: %s, c_date: %s, b_date: %s",
                            A.f0, C.f0, Bdates);
                }
        );

        // Replace print() with custom SinkFunction
//        input.map(new MapFunction<Tuple3<String, Double, String>, Tuple3<String, Double, String>>() {
//            @Override
//            public Tuple3<String, Double, String> map(Tuple3<String, Double, String> event) {
//                System.out.println("Processed Event: " + event);
//                return event;
//            }
//        }).addSink(new SinkFunction<Tuple3<String, Double, String>>() {
//            @Override
//            public void invoke(Tuple3<String, Double, String> value, Context context) {
//                System.out.println(value);
//            }
//        });

        // Collect results and print them in a controlled way
        resultStream.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println("Pattern Match: " + value);
            }
        });

        // Execute the Flink program
        env.execute("Crime Pattern Detection");

        // Final statistics
        long endTime = System.currentTimeMillis();
        long totalRuntime = endTime - startTime;

        System.out.println("Engine used: FLINKCEP");
        System.out.println("Total Running Time: " + totalRuntime / 1000.0 + " seconds");
        System.out.println("Number Of Events Processed: " + eventCount[0]);
        System.out.println("Number Of Matches Found: " + matchCount[0]);
        System.out.println("Maximum Latency in nano: " + maxLatency[0] * 1_000_000);
        System.out.println("Minimum Latency in nano: " + minLatency[0] * 1_000_000);
        System.out.println("Average Latency in nano: " + (matchCount[0] > 0 ? (totalLatency[0] / matchCount[0]) * 1_000_000 : 0));
        System.out.println("Throughput: " + (eventCount[0] / (totalRuntime / 1000.0)) + " events/second");

        Runtime runtime = Runtime.getRuntime();
        runtime.gc();
        long memory_cost = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Memory cost: " + memory_cost);
    }
}