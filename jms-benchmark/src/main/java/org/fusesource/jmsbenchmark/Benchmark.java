/**
 * Copyright (C) 2009-2015 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.jmsbenchmark;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.airline.*;
import org.ocpsoft.prettytime.PrettyTime;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.fusesource.jmsbenchmark.Support.*;

public class Benchmark {


    static final ObjectMapper MAPPER = new ObjectMapper();

    {
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }


    static TreeMap<String, Object> parseScenarioName(String name) {
        try {
            String json = "{" + name + "}";
            return MAPPER.readValue(json, TreeMap.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("benchmark")
                .withDescription("JMS Benchmark")
                .withCommand(Help.class)
                .withCommand(Run.class)
                .withDefaultCommand(Help.class);
        Cli<Runnable> parser = builder.build();
        try {
            parser.parse(args).run();
        } catch (ParseArgumentsUnexpectedException e) {
            System.err.println(e.getMessage());
            System.out.println();
            parser.parse("help").run();
        }
    }

    static public class ScenarioReport {
        public TreeMap<String, Object> parameters = new TreeMap<String, Object>();
        public long[] timestamp = new long[]{};
        @JsonProperty("producer tp")
        public long[] producerTp = new long[]{};
        @JsonProperty("consumer tp")
        public long[] consumerTp = new long[]{};
        @JsonProperty("max latency")
        public long[] maxLatency = new long[]{};
        public long[] errors = new long[]{};
    }

    static public class BenchmarkReport {
        @JsonProperty("benchmark_settings")
        TreeMap<String, Object> benchmarkSettings = new java.util.TreeMap<>();
        @JsonProperty("scenarios")
        ScenarioReport[] scenarios = new ScenarioReport[]{};
    }


    @Command(name = "run", description = "creates a new broker instance")
    public static class Run implements Runnable {


        @Option(name = "--continue", description = "Should we continue from the last run?  Avoid re-running previously run scenarios.")
        boolean resume = true;

        @Option(name = "--provider", description = "The type of provider being benchmarked.")
        String provider = "activemq";

        @Option(name = "--broker-name", description = "The name of the broker being benchmarked.")
        String brokerName;

        @Option(name = "--url", description = "server url")
        String url = "tcp://127.0.0.1:61616";

        @Option(name = "--user-name", description = "login name to connect with")
        String userName = null;
        @Option(name = "--password", description = "password to connect with")
        String password = null;

        @Option(name = "--sample-count", description = "number of samples to take")
        int sampleCount = 15;
        @Option(name = "--sample-interval", description = "number of milli seconds that data is collected.")
        int sampleInterval = 1000;
        @Option(name = "--warm-up-count", description = "number of warm up samples to ignore")
        int warmUpCount = 3;

        @Arguments(title = "out", description = "The file to store benchmark metrics in", required = true)
        File out;

        @Option(name = "--queue-prefix", description = "prefix used for queue names.")
        String queuePrefix = "";
        @Option(name = "--topic-prefix", description = "prefix used for topic names.")
        String topicPrefix = "";

        @Option(name = "--drain-timeout", description = "How long to wait for a drain to timeout in ms.")
        long drainTimeout = 3000L;

        @Option(name = "--display-errors", description = "Should errors get dumped to the screen when they occur?")
        boolean displayErrors = false;

        @Option(name = "--allow-worker-interrupt", description = "Should worker threads get interrupted if they fail to shutdown quickly?")
        boolean allowWorkerInterrupt = false;

        @Option(name = "--skip", description = "Comma separated list of tests to skip.")
        String skip = "";

        @Option(name = "--max-clients", description = "Max number of clients to run in parallel")
        int maxClients = 200;

        @Option(name = "--max-destinations", description = "Max number of destinations to use")
        int maxDestinations = 100;

        @Option(name = "--show-skips", description = "Should skipped scenarios be displayed.")
        boolean showSkips = false;

        <T> String jsonFormat(List<T> value) {
            return "[ " + mkString(value, ", ") + " ]";
        }

        TreeMap<String, Object> benchmarkSettings = new TreeMap<>();
        LinkedHashMap<TreeMap<String, Object>, ScenarioReport> scenarioReports = new LinkedHashMap<>();

        boolean haveScenarioReport(String name) {
            TreeMap<String, Object> parameters = parseScenarioName(name);
            return scenarioReports.containsKey(parameters);
        }

        void addScenarioReport(String name, List<DataSample> samples) {

            try {
                ScenarioReport scenario = new ScenarioReport();
                scenario.parameters = parseScenarioName(name);
                scenario.timestamp = samples.stream().mapToLong(DataSample::getTime).toArray();
                scenario.consumerTp = samples.stream().mapToLong(DataSample::getConsumed).toArray();
                scenario.producerTp = samples.stream().mapToLong(DataSample::getProduced).toArray();
                scenario.maxLatency = samples.stream().mapToLong(DataSample::getMaxLatency).toArray();
                scenario.errors = samples.stream().mapToLong(DataSample::getErrors).toArray();

                scenarioReports.put(scenario.parameters, scenario);
                BenchmarkReport report = new BenchmarkReport();
                report.benchmarkSettings = benchmarkSettings;
                report.scenarios = scenarioReports.values().stream().toArray(x -> new ScenarioReport[x]);
                MAPPER.writerWithDefaultPrettyPrinter().writeValue(out, report);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {

            if (brokerName == null) {
                brokerName = stripSuffix(out.getName(), ".json");
            }

            if (out.getParentFile() != null) {
                out.getParentFile().mkdirs();
            }

            // Load up the previous results so that we can just update the result file.
            if (out.exists()) {
                try {
                    BenchmarkReport report = MAPPER.readValue(out, BenchmarkReport.class);
                    benchmarkSettings = report.benchmarkSettings;
                    for (ScenarioReport scenario : report.scenarios) {
                        scenarioReports.put(scenario.parameters, scenario);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            if (!benchmarkSettings.containsKey(brokerName)) {
                benchmarkSettings.put("broker_name", brokerName);
            }
            benchmarkSettings.put("url", url);
            benchmarkSettings.put("warm_up_count", warmUpCount);

            System.out.println("===================================================================");
            System.out.println("Benchmarking %s at: %s".format(brokerName, url));
            System.out.println("===================================================================");
            runBenchmarks();
        }

        private void benchmark(String name, Consumer<Scenario> initFunc) {
            benchmark(name, true, sampleCount, null, initFunc);
        }

        private void benchmark(String name, boolean drain, int sc, Function<List<Scenario>, Boolean> isDone, Consumer<Scenario> initFunc) {
            try {
                multiBenchmark(Arrays.asList(name), drain, sc, isDone, scenarios -> initFunc.accept(scenarios.get(0)));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        static final HashMap<String, String> PROVIDER_SHORT_NAMES = new HashMap<>();

        static {
            PROVIDER_SHORT_NAMES.put("activemq", "org.fusesource.jmsbenchmark.ActiveMQScenario");
            PROVIDER_SHORT_NAMES.put("stomp", "org.fusesource.jmsbenchmark.StompScenario");
        }

        JMSClientScenario createScenario() {
            String clazz = PROVIDER_SHORT_NAMES.get(provider);
            if (clazz == null) {
                clazz = provider;
            }
            try {
                return (JMSClientScenario) getClass().getClassLoader().loadClass(clazz).newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Runs all the listed scenarios concurrently then finally runs the provided proc
         * once they are under load.
         */
        <T> void withLoad(List<Scenario> s, Supplier<T> proc) throws TimeoutException, InterruptedException {

            final int[] i = new int[]{0};
            Supplier<Long> timeout = () -> {
                i[0] += 1;
                if (i[0] <= 10 * s.size()) {
                    System.out.print("c");
                    return 1000L;
                } else {
                    return 0L;
                }
            };

            LinkedList<Scenario> remaining = new LinkedList<>(s);
            while (!remaining.isEmpty()) {
                Scenario scenario = remaining.removeLast();
                final Supplier<T> nextProc = proc;
                proc = () -> {
                    try {
                        return scenario.withLoadAndConnectTimeout(nextProc, timeout);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
            }

            proc.get();
        }

        private static void sleep(int amount) {
            try {
                Thread.sleep(amount);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void multiBenchmark(List<String> names, boolean drain, int sc, Function<List<Scenario>, Boolean> isDone, Consumer<List<Scenario>> initFunc) throws InterruptedException {
            Runtime.getRuntime().gc();
            List<Scenario> scenarios = names.stream().map(name -> {
                Scenario scenario = createScenario();
                scenario.name = name;
                scenario.sampleInterval = sampleInterval;
                scenario.url = url;
                scenario.userName = userName;
                scenario.password = password;
                scenario.queuePrefix = queuePrefix;
                scenario.topicPrefix = topicPrefix;
                scenario.drainTimeout = drainTimeout;
                scenario.displayErrors = displayErrors;
                scenario.allowWorkerInterrupt = allowWorkerInterrupt;
                return scenario;
            }).collect(Collectors.toList());

            initFunc.accept(scenarios);

            scenarios.forEach(scenario -> {
                scenario.setDestinationName("queue".equals(scenario.destinationType) ? "loadq" : "loadt");
            });

            System.out.printf("starting scenario : %s \n", mkString(names, " and "));
            System.out.print("sampling : ");


            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            try {
                withLoad(scenarios, () -> {
                    for (int i = 0; i < warmUpCount; i++) {
                        System.out.print("w");
                        sleep(sampleInterval);
                    }
                    scenarios.forEach(x -> x.collectionStart());
                    if (isDone != null) {
                        while (!isDone.apply(scenarios)) {
                            System.out.print(".");
                            sleep(sampleInterval);
                            scenarios.forEach(x -> x.collectionSample());
                        }

                    } else {
                        int remaining = sc;
                        while (remaining > 0) {
                            System.out.print(".");
                            int sampleInterval1 = sampleInterval;
                            sleep(sampleInterval1);
                            scenarios.forEach(x -> x.collectionSample());
                            remaining--;
                        }
                    }

                    System.out.println();
                    scenarios.forEach(
                            scenario -> {
                                ArrayList<DataSample> collected = scenario.collectionEnd();
                                if (collected.stream().anyMatch(x -> x.produced != 0)) {
                                    List<Long> throughputs = collected.stream().map(x -> x.produced).collect(Collectors.toList());
                                    System.out.println("producer throughput samples : %s".format(jsonFormat(throughputs)));
                                }
                                if (collected.stream().anyMatch(x -> x.consumed != 0)) {
                                    List<Long> throughputs = collected.stream().map(x -> x.consumed).collect(Collectors.toList());
                                    System.out.println("consumer throughput samples : %s".format(jsonFormat(throughputs)));
                                    List<String> latencies = collected.stream().map(x -> String.format("%.3f ms", x.maxLatency / 1000000.0)).collect(Collectors.toList());
                                    System.out.println("consumer max latency samples: %s".format(jsonFormat(latencies)));
                                }
                                if (collected.stream().anyMatch(x -> x.errors != 0)) {
                                    List<Long> errors = collected.stream().map(x -> x.errors).collect(Collectors.toList());
                                    System.out.println("errors                      : %s".format(jsonFormat(errors)));
                                }
                                addScenarioReport(scenario.name, collected);
                            });
                    return 0;
                });
            } catch (TimeoutException e) {
                System.out.println(e.getMessage());
            }
            Thread.currentThread().setPriority(Thread.NORM_PRIORITY);

            if (drain) {
                for (Scenario scenario : scenarios) {
                    scenario.drain();
                }
            }
        }


        class ScenarioDescription {
            public final String name;
            public final Runnable execute;
            public final int duration;

            ScenarioDescription(String name, Runnable execute) {
                this(name, execute, ((sampleCount + warmUpCount + 2) * sampleInterval) + 2000);
            }

            ScenarioDescription(String name, Runnable execute, int duration) {
                this.name = name;
                this.execute = execute;
                this.duration = duration;
            }
        }

        ArrayList<Integer> createIncreasingIntList(int initial, int pow, int max) {
            LinkedList<Integer> clientCounts = new LinkedList<>();
            clientCounts.add(initial);
            while ((clientCounts.getFirst() * pow) < max) {
                clientCounts.addFirst(clientCounts.getFirst() * pow);
            }
            return new ArrayList<>(clientCounts);
        }

        void runBenchmarks() {

            Set<String> scenariosToSkip = Arrays.asList(skip.split(",")).stream().map(x -> x.trim()).collect(Collectors.toSet());
            ArrayList<ScenarioDescription> descriptions = new ArrayList<>();


            ArrayList<Integer> clientCounts = createIncreasingIntList(1, 10, maxClients);
            ArrayList<Integer> destinationsCounts = createIncreasingIntList(1, 10, maxDestinations);

            //
            // Standard throughput scenarios.
            for (Boolean persistent : Arrays.asList(false, true)) {
                for (String mode : Arrays.asList("queue", "topic")) {
                    for (Integer producers : clientCounts) {
                        for (Integer destinationCount : destinationsCounts) {
                            for (Integer consumers : clientCounts) {
                                for (Integer messageSize : Arrays.asList(10, 100, 1000, 100000, 10000000)) {
                                    for (Integer txSize : Arrays.asList(0, 1, 10, 100)) {


                                        String name = String.format("\"group\": \"throughput\", \"mode\": \"%s\", \"persistent\": %s, \"message_size\": %s, \"tx_size\": %s, \"destination_count\": %s, \"consumers\": %s, \"producers\": %s", mode, persistent, messageSize, txSize, destinationCount, consumers, producers);
                                        int scalingDimensions = 0;
                                        if (messageSize > 10) scalingDimensions += 1;
                                        if (txSize > 0) scalingDimensions += 1;

                                        if (producers == consumers && destinationCount == 1) {
                                        } else if (producers == consumers && producers == destinationCount) {
                                        } else {
                                            if (producers > 1) scalingDimensions += 1;
                                            if (destinationCount > 1) scalingDimensions += 1;
                                            if (consumers > 1) scalingDimensions += 1;
                                        }

                                        String skip = null;
                                        if (haveScenarioReport(name)) {
                                            skip = "Already have the results";
                                        } else if (scenariosToSkip.contains("throughput")) {
                                            skip = "--skip command line option";
                                        }
                                        // Skip on odds scenario combinations like more destinations than clients.
                                        else if ((consumers < destinationCount) || (producers < destinationCount)) {
                                            skip = "more destinations than clients";
                                        } else if (consumers + producers > maxClients) {
                                            skip = "--max-clients exceeded";
                                        }
                                        // When using lots of clients, only test against small txs and small messages.
                                        else if (scalingDimensions > 1) {
                                            skip = "Scales in more than one dimension.";
                                        }

                                        if (skip != null) {
                                            if (showSkips) {
                                                System.out.println();
                                                System.out.println("skipping  : " + name);
                                                System.out.println("   reason : " + skip);
                                            }
                                            continue;
                                        }
                                        descriptions.add(new ScenarioDescription(name, () -> {
                                            benchmark(name, (g) -> {
                                                g.destinationType = mode;
                                                g.persistent = persistent;
                                                g.durable = persistent && "topic".equals(mode);
                                                g.ackMode = persistent ? "client" : "dups_ok";
                                                g.messageSize = messageSize;
                                                g.txSize = txSize;
                                                g.destinationCount = destinationCount;
                                                g.consumers = consumers;
                                                g.producers = producers;
                                            });
                                        }));
                                    }
                                }
                            }
                        }
                    }
                }

            }

            //
            // Slow consumer scenarios.
            for (String mode : Arrays.asList("queue", "topic")) {
                for (Boolean persistent : Arrays.asList(false, true)) {
                    String name = String.format("\"group\": \"slow_consumer\", \"mode\": \"%s\", \"persistent\": %s", mode, persistent);

                    String skip = null;
                    if (haveScenarioReport(name)) {
                        skip = "Already have the results";
                    } else if (scenariosToSkip.contains("slow_consumer")) {
                        skip = "--skip command line option";
                    }

                    if (skip != null) {
                        if (showSkips) {
                            System.out.println();
                            System.out.println("skipping  : " + name);
                            System.out.println("   reason : " + skip);
                        }
                        continue;
                    }
                    descriptions.add(new ScenarioDescription(name, () -> {
                        benchmark(name, (g) -> {
                            g.destinationType = mode;
                            g.persistent = persistent;
                            g.durable = persistent && "topic".equals(mode);
                            g.ackMode = persistent ? "client" : "auto";
                            g.messageSize = 10;
                            g.producers = 1;
                            g.consumers = 10;
                            g.consumerSleep = new SleepTask() {
                                @Override
                                void apply(Scenario.Client client) throws InterruptedException {
                                    // the client /w id 2 will be the slow one.
                                    if (client.id() == 2) {
                                        Thread.sleep(500);
                                    }
                                }
                            };
                        });
                    }));
                }
            }


            // Latency scenarios.
            for (Integer producerRate : Arrays.asList(100, 1000, 1000 * 10, 1000 * 100, 1000 * 1000)) {

                String name = String.format("\"group\": \"latency\", \"producer_rate\": %d", producerRate);

                String skip = null;
                if (haveScenarioReport(name)) {
                    skip = "Already have the results";
                } else if (scenariosToSkip.contains("latency")) {
                    skip = "--skip command line option";
                }

                if (skip != null) {
                    if (showSkips) {
                        System.out.println();
                        System.out.println("skipping  : " + name);
                        System.out.println("   reason : " + skip);
                    }
                    continue;
                }
                descriptions.add(new ScenarioDescription(name, () -> {
                    benchmark(name, (g) -> {

                        g.destinationType = "queue";
                        g.persistent = false;
                        g.durable = false;
                        g.ackMode = "dups_ok";
                        g.messageSize = 10;
                        g.producers = 1;
                        g.consumers = 1;
                        g.producerSleep = new SleepTask() {
                            long initTS = 0L;
                            long sent = 0L;

                            @Override
                            void init(Long time) {
                                initTS = System.nanoTime();
                            }

                            @Override
                            void apply(Scenario.Client client) throws InterruptedException {
                                sent += 1;
                                long now = System.nanoTime();
                                long elapsed = now - initTS;

                                long shouldHaveSent = producerRate * elapsed / TimeUnit.SECONDS.toNanos(1);
                                if (shouldHaveSent <= sent) {
                                    // Producer has sent it's fair share.. sleep till we need to send again.
                                    if (producerRate >= 1000) {
                                        Thread.sleep(1);
                                    } else {
                                        Thread.sleep(Math.max(1000L / producerRate, 1));
                                    }
                                }
                            }

                        };
                    });
                }));
            }

            // Load up a queue for 30 seconds..
            int loadUnloadSamples = 60;
            for (Boolean persistent : Arrays.asList(false, true)) {

                String name = String.format("\"group\": \"queue_staging\", \"persistent\": %s", persistent);
                if (haveScenarioReport(name)) {
                    skip = "Already have the results";
                } else if (scenariosToSkip.contains("queue_staging")) {
                    skip = "--skip command line option";
                }

                if (skip != null) {
                    if (showSkips) {
                        System.out.println();
                        System.out.println("skipping  : " + name);
                        System.out.println("   reason : " + skip);
                    }
                    continue;
                }

                descriptions.add(new ScenarioDescription(name, () -> {
                    benchmark(name, true, loadUnloadSamples, null, (g) -> {
                        g.destinationType = "queue";
                        g.persistent = persistent;
                        g.ackMode = "auto";
                        g.messageSize = 10;
                        g.txSize = 0;
                        g.producers = 10;
                        g.consumers = 10;

                        // producer will sleep midway..
                        g.producerSleep = new SleepTask() {
                            long start = 0L;

                            @Override
                            void init(Long time) {
                                start = time;
                            }

                            @Override
                            void apply(Scenario.Client client) throws InterruptedException {
                                long elapsed = System.currentTimeMillis() - start;
                                long midpoint = (warmUpCount + (loadUnloadSamples / 2)) * sampleInterval;
                                if (elapsed > midpoint) {
                                    client.shutdown();
                                }
                            }

                        };

                        // consumer will sleep until midway through the scenario.
                        g.consumerSleep = new SleepTask() {
                            long start = 0L;

                            @Override
                            void init(Long time) {
                                start = time;
                            }

                            @Override
                            void apply(Scenario.Client client) throws InterruptedException {
                                long elapsed = System.currentTimeMillis() - start;
                                long midpoint = (warmUpCount + (loadUnloadSamples / 2)) * sampleInterval;
                                if (elapsed < midpoint) {
                                    Thread.sleep(midpoint - elapsed);
                                }
                            }
                        };
                    });
                }, ((loadUnloadSamples + warmUpCount) * sampleInterval) + 2000));
            }

            int remaining = descriptions.size();
            long timeRemaining = descriptions.stream().mapToLong(x -> x.duration).sum();
            PrettyTime prettyTime = new PrettyTime();
            for (ScenarioDescription description : descriptions) {
                System.out.println();
                System.out.println();
                System.out.printf("%d scenarios remaining. Estimating completion at: %s.\n", remaining, prettyTime.format(new Date(System.currentTimeMillis() + (timeRemaining))));
                description.execute.run();
                remaining -= 1;
                timeRemaining -= description.duration;
            }

        }

    }

}

