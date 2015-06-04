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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import static org.fusesource.jmsbenchmark.Support.*;

abstract public class Scenario {

    long NANOS_PER_SECOND = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);

    public static class Entry<A, B> extends AbstractMap.SimpleImmutableEntry<A, B> {
        public Entry(A key, B value) {
            super(key, value);
        }
    }

    String url = "tcp://localhost:61616";
    String userName;
    String password;

    SleepTask producerSleep = new SleepTask();
    void setProducerSleep(int value) {
        producerSleep = new FixedSleepTask(value);
    }

    SleepTask consumerSleep = new SleepTask();
    void setConsumerSleep(int value) {
        consumerSleep = new FixedSleepTask(value);
    }

    boolean allowWorkerInterrupt = false;
    boolean useMessageListener = false;

    int producers = 1;
    int producersPerSample = 0;

    int consumers = 1;
    int consumersPerSample = 0;
    int sampleInterval = 1000;

    int messageSize = 1024;
    boolean persistent = false;

    ArrayList<ArrayList<Entry<String, String>>> headers = new ArrayList<>();
    String selector = null;
    boolean noLocal = false;
    boolean durable = false;
    String ackMode = "auto";
    long messagesPerConnection = -1L;
    boolean displayErrors = false;
    int txSize = 0;

    String destinationType = "queue";
    Supplier<String> destinationName = () -> "load";

    String getDestinationName() {
        return destinationName.get();
    }
    void setDestinationName(String value) {
        destinationName = () -> value;
    }
    void setDestinationName(Supplier<String> value) {
        destinationName = value;
    }

    int destinationCount;

    AtomicLong producerCounter = new AtomicLong();
    AtomicLong consumerCounter = new AtomicLong();
    AtomicLong errorCounter = new AtomicLong();
    AtomicLong maxLatency = new AtomicLong();
    AtomicBoolean done = new AtomicBoolean();

    String queuePrefix = "";
    String topicPrefix = "";
    String name = "custom";

    long drainTimeout = 2000L;

    void run() throws TimeoutException, InterruptedException {
        System.out.print(toString());
        System.out.println("--------------------------------------");
        System.out.println("     Running: Press ENTER to stop");
        System.out.println("--------------------------------------");
        System.out.println("");

        withLoad(() -> {

            // start a sampling client...
            Thread sampleThread = new Thread() {

                void printRate(String name, long periodCount, long totalCount, long nanos) {
                    java.lang.Float ratePerSecond = ((1.0f * periodCount / nanos) * NANOS_PER_SECOND);
                    System.out.println("%s total: %,d, rate: %,.3f per second".format(name, totalCount, ratePerSecond));
                }

                public void run() {


                    try {
                        long start = System.nanoTime();
                        long totalProducerCount = 0L;
                        long totalConsumerCount = 0L;
                        long totalErrorCount = 0L;
                        collectionStart();
                        while (!done.get()) {
                            Thread.sleep(sampleInterval);
                            long end = System.nanoTime();
                            collectionSample();
                            ArrayList<DataSample> collected = collectionEnd();

                            if (producers > 0) {
                                long count = Support.last(collected).produced;
                                totalProducerCount += count;
                                if (totalProducerCount > 0) {
                                    printRate("Producer", count, totalProducerCount, end - start);
                                }
                            }

                            if (consumers > 0) {
                                long count = Support.last(collected).consumed;
                                totalConsumerCount += count;
                                if (totalConsumerCount > 0) {
                                    printRate("Consumer", count, totalConsumerCount, end - start);
                                }
                            }

                            long count = Support.last(collected).errors;
                            totalErrorCount += count;
                            if (totalErrorCount > 0) {
                                if (count != 0) {
                                    printRate("Error", count, totalErrorCount, end - start);
                                }
                            }

                            start = end;
                        }
                    } catch (InterruptedException e) {
                    }
                }
            };
            try {
                sampleThread.start();
                System.in.read();
                done.set(true);
                sampleThread.interrupt();
                sampleThread.join();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            return null;
        });

    }

    @Override
    public String toString() {
        return "--------------------------------------\n" +
                "Scenario Settings\n" +
                "--------------------------------------\n" +
                "  destinationType       = " + destinationType + "\n" +
                "  queuePrefix           = " + queuePrefix + "\n" +
                "  topicPrefix           = " + topicPrefix + "\n" +
                "  destinationCount      = " + destinationCount + "\n" +
                "  destinationName       = " + getDestinationName() + "\n" +
                "  sampleInterval (ms)   = " + sampleInterval + "\n" +
                "  \n" +
                "  --- Producer Properties ---\n" +
                "  producers             = " + producers + "\n" +
                "  messageSize           = " + messageSize + "\n" +
                "  persistent            = " + persistent + "\n" +
                "  producerSleep (ms)    = " + producerSleep.get() + "\n" +
                "  headers               = " + mkString(headers, ", ") + "\n" +
                "  \n" +
                "  --- Consumer Properties ---\n" +
                "  consumers             = " + consumers + "\n" +
                "  consumerSleep (ms)    = " + consumerSleep.get() + "\n" +
                "  selector              = " + selector + "\n" +
                "  durable               = " + durable + "\n" +
                "";
    }

    protected ArrayList<Entry<String, String>> headersFor(int i) {
        if (headers.isEmpty()) {
            return new ArrayList<>();
        } else {
            return headers.get(i % headers.size());
        }
    }

    ArrayList<DataSample> dataSamples = new ArrayList<>();

    void collectionStart() {
        producerCounter.set(0);
        consumerCounter.set(0);
        errorCounter.set(0);
        maxLatency.set(0);
        dataSamples = new ArrayList<>();
    }

    void updateMaxLatency(long value) {
        long was = maxLatency.get();
        while (value > was && !maxLatency.compareAndSet(was, value)) {
            // someone else changed it before we could update it, lets
            // get the update and see if we still need to update.
            was = maxLatency.get();
        }
    }

    ArrayList<DataSample> collectionEnd() {
        return new ArrayList<>(dataSamples);
    }

    public static interface Client {
        int id();

        void start();

        void shutdown();

        void waitForShutdown() throws InterruptedException;
    }

    ArrayList<Client> producerClients = new ArrayList<>();
    ArrayList<Client> consumerClients = new ArrayList<>();

    CountDownLatch connectedLatch = null;
    CountDownLatch applyLoadLatch = null;

    <T> T withLoad(Supplier<T> func) throws TimeoutException, InterruptedException {
        final int[] i = new int[]{0};
        return withLoadAndConnectTimeout(func, () -> {
            i[0] += 1;
            if (i[0] <= 10) {
                return 1000L;
            } else {
                return 0L;
            }
        });
    }

    <T> T withLoadAndConnectTimeout(Supplier<T> func, Supplier<Long> continueConnecting) throws TimeoutException, InterruptedException {
        done.set(false);

        producerSleep.init(System.currentTimeMillis());
        consumerSleep.init(System.currentTimeMillis());

        connectedLatch = new CountDownLatch(producers + consumers);
        applyLoadLatch = new CountDownLatch(1);

        for (int i = 0; i < producers; i++) {
            Client client = createProducer(i);
            producerClients.add(client);
            client.start();
        }

        for (int i = 0; i < consumers; i++) {
            Client client = createConsumer(i, false);
            consumerClients.add(client);
            client.start();
        }

        try {
            long timeout = 0L;
            boolean doneConnect = false;
            while (!doneConnect) {
                if (connectedLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                    doneConnect = true;
                    applyLoadLatch.countDown();
                } else {
                    timeout = continueConnecting.get();
                    if (timeout <= 0) {
                        applyLoadLatch.countDown();
                        done.set(true);
                        throw new TimeoutException("All clients could not connect.  Failing connections: " + connectedLatch.getCount());
                    }
                }
            }
            return func.get();

        } finally {
            done.set(true);
            for (Client client : producerClients) {
                client.shutdown();
            }
            for (Client client : producerClients) {
                client.waitForShutdown();
            }
            producerClients = new ArrayList<>();
            // wait for the threads to finish..
            for (Client client : consumerClients) {
                client.shutdown();
            }
            for (Client client : consumerClients) {
                client.waitForShutdown();
            }
            consumerClients = new ArrayList<>();
        }
    }

    void drain() throws InterruptedException {
        done.set(false);
        if ("queue".equals(destinationType) || "raw_queue".equals(destinationType) || durable == true) {
            System.out.print("draining");
            consumerCounter.set(0);
            final long[] drained = new long[]{ 0L };

            int originalTxSize = txSize;
            SleepTask originalConsumerSleep = consumerSleep;
            String originalAckMode = ackMode;

            // Lets change the consumer config to consume as fast as possible.
            txSize = 0;
            setConsumerSleep(0);
            ackMode = "dups_ok";

            ArrayList<Client> consumerClients = new ArrayList<Client>();

            int drainingConsumers = "queue".equals(destinationType) ? destinationCount : consumers;
            for (int i = 0; i < drainingConsumers; i++) {
                Client client = createConsumer(i, true);
                consumerClients.add(client);
                client.start();
            }

            // Keep sleeping until we stop draining messages.
            try {
                Thread.sleep(drainTimeout);
                Supplier<Boolean> done = () -> {
                        long c = consumerCounter.getAndSet(0);
                        drained[0] += c;
                        return c == 0;
                };
                while (!done.get()) {
                    System.out.print(".");
                    Thread.sleep(drainTimeout);
                }
            } finally {
                done.set(true);
                for (Client client : consumerClients) {
                    client.shutdown();
                }
                for (Client client : consumerClients) {
                    client.waitForShutdown();
                }
                System.out.printf(". (drained %d)", drained[0]);
                txSize = originalTxSize;
                consumerSleep = originalConsumerSleep;
                ackMode = originalAckMode;
            }
        }
    }


    void collectionSample() {

        long now = System.currentTimeMillis();
        DataSample data = new DataSample(now,
                producerCounter.getAndSet(0),
                consumerCounter.getAndSet(0),
                errorCounter.getAndSet(0),
                maxLatency.getAndSet(0)
        );
        dataSamples.add(data);

        // we might need to increment number the producers..
        for (int i = 0; i < producersPerSample; i++) {
            Client client = createProducer(producerClients.size());
            producerClients.add(client);
            client.start();
        }

        // we might need to increment number the consumers..
        for (int i = 0; i < consumersPerSample; i++) {
            Client client = createConsumer(consumerClients.size(), false);
            consumerClients.add(client);
            client.start();
        }

    }

    abstract Client createProducer(int i);

    abstract Client createConsumer(int i, boolean drain);

}

