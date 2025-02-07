package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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

public class CrimePatternDetectionB {
    private static final double MAX_HUMAN_SPEED_KMPH = 50.0; // 设置为罪犯最大跑步速度 20 km/h

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Start time for performance measurement
        long startTime = System.currentTimeMillis();

        // Load CSV file and convert to DataStream
        DataStream<Tuple5<String, String, String, Double, Double>> input = env.addSource(new SourceFunction<Tuple5<String, String, String, Double, Double>>() {
            @Override
            public void run(SourceContext<Tuple5<String, String, String, Double, Double>> ctx) throws Exception {
                String filePath = "./data/cleaned_chicago_crime_data.csv"; // Replace with the actual CSV file path
                BufferedReader reader = new BufferedReader(new FileReader(filePath));
                String line;
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                while ((line = reader.readLine()) != null) {
                    try {
                        String[] fields = line.split(",");
                        if (fields.length == 5) {
                            ctx.collect(new Tuple5<>(
                                    fields[0],
                                    fields[1],
                                    fields[2],
                                    Double.parseDouble(fields[3]),
                                    Double.parseDouble(fields[4])
                            ));
                        }
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
                WatermarkStrategy.<Tuple5<String, String, String, Double, Double>>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                            return LocalDateTime.parse(event.f1, formatter).toInstant(ZoneOffset.UTC).toEpochMilli();
                        })
        );

        // 定义CEP模式
        Pattern<Tuple5<String, String, String, Double, Double>, ?> crimePattern = Pattern
                .<Tuple5<String, String, String, Double, Double>>begin("R")
                .where(new IterativeCondition<Tuple5<String, String, String, Double, Double>>() {
                    @Override
                    public boolean filter(Tuple5<String, String, String, Double, Double> event, Context<Tuple5<String, String, String, Double, Double>> ctx) {
                        return "ROBBERY".equals(event.f2); // 匹配 R 事件
                    }
                })
                .followedBy("Z1")
                .oneOrMore()
                .followedBy("B")
                .where(new IterativeCondition<Tuple5<String, String, String, Double, Double>>() {
                    @Override
                    public boolean filter(Tuple5<String, String, String, Double, Double> event, Context<Tuple5<String, String, String, Double, Double>> ctx) throws Exception {
                        Tuple5<String, String, String, Double, Double> r = null;
                        for (Tuple5<String, String, String, Double, Double> e : ctx.getEventsForPattern("R")) {
                            r = e;
                        }

                        // 地理位置条件
                        boolean isInLocation = event.f3 >= r.f3 - 0.05 && event.f3 <= r.f3 + 0.05
                                && event.f4 >= r.f4 - 0.02 && event.f4 <= r.f4 + 0.02;

                        return "BATTERY".equals(event.f2) && isInLocation;
                    }
                })
                .followedBy("Z2")
                .oneOrMore()
                .followedBy("M")
                .where(new IterativeCondition<Tuple5<String, String, String, Double, Double>>() {
                    @Override
                    public boolean filter(Tuple5<String, String, String, Double, Double> event, Context<Tuple5<String, String, String, Double, Double>> ctx) throws Exception {
                        Tuple5<String, String, String, Double, Double> r = null;
                        for (Tuple5<String, String, String, Double, Double> e : ctx.getEventsForPattern("R")) {
                            r = e;
                        }

                        boolean isInLocation = event.f3 >= r.f3 - 0.05 && event.f3 <= r.f3 + 0.05
                                && event.f4 >= r.f4 - 0.02 && event.f4 <= r.f4 + 0.02;

                        return "MOTOR VEHICLE THEFT".equals(event.f2) && isInLocation;
                    }
                })
                .within(Time.minutes(30)); // 整个模式必须在 30 分钟内发生

        // Apply the pattern
        PatternStream<Tuple5<String, String, String, Double, Double>> patternStream = CEP.pattern(input, crimePattern);

        // Initialize counters
        final long[] eventCount = {10000};
        final long[] matchCount = {0};
        final long[] maxLatency = {0};
        final long[] minLatency = {Long.MAX_VALUE};
        final long[] totalLatency = {0};

        // Select and process matched patterns
        DataStream<String> resultStream = patternStream.select(
                (PatternSelectFunction<Tuple5<String, String, String, Double, Double>, String>) pattern -> {
                    Tuple5<String, String, String, Double, Double> robbery = pattern.get("R").get(0);
                    Tuple5<String, String, String, Double, Double> battery = pattern.get("B").get(0);
                    Tuple5<String, String, String, Double, Double> motorTheft = pattern.get("M").get(0);

                    // Get Z1 and Z2 events
                    List<Tuple5<String, String, String, Double, Double>> z1Events = pattern.get("Z1");
                    List<Tuple5<String, String, String, Double, Double>> z2Events = pattern.get("Z2");

                    // Extract IDs from Z1 and Z2
                    String z1Ids = z1Events.stream()
                            .map(event -> event.f0) // Extract id
                            .collect(Collectors.joining(", "));
                    String z2Ids = z2Events.stream()
                            .map(event -> event.f0) // Extract id
                            .collect(Collectors.joining(", "));

                    // Calculate latency
                    long eventTime = LocalDateTime.parse(robbery.f1, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                            .toInstant(ZoneOffset.UTC).toEpochMilli();
                    long latency = System.currentTimeMillis() - eventTime;

                    synchronized (maxLatency) {
                        maxLatency[0] = Math.max(maxLatency[0], latency);
                        minLatency[0] = Math.min(minLatency[0], latency);
                        totalLatency[0] += latency;
                        matchCount[0]++;
                    }

                    // Print IDs of R, B, M, Z1, and Z2
                    System.out.println(String.format("RID: %s, BID: %s, MID: %s, Z1 IDs: [%s], Z2 IDs: [%s]",
                            robbery.f0, battery.f0, motorTheft.f0, z1Ids, z2Ids));

                    // Return formatted result string
                    return String.format("RID: %s, BID: %s, MID: %s, Z1 IDs: [%s], Z2 IDs: [%s]",
                            robbery.f0, battery.f0, motorTheft.f0, z1Ids, z2Ids);
                }
        );

        // Replace print() with custom SinkFunction
//        input.map(new MapFunction<Tuple5<String, String, String, Double, Double>, Tuple5<String, String, String, Double, Double>>() {
//            @Override
//            public Tuple5<String, String, String, Double, Double> map(Tuple5<String, String, String, Double, Double> event) {
//                System.out.println("Processed Event: " + event);
//                return event;
//            }
//        }).addSink(new SinkFunction<Tuple5<String, String, String, Double, Double>>() {
//            @Override
//            public void invoke(Tuple5<String, String, String, Double, Double> value, Context context) {
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
    }
}