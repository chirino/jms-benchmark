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

import javax.jms.*;
import java.util.concurrent.atomic.AtomicBoolean;

abstract public class JMSClientScenario extends Scenario {


    Client createProducer(int i) {
        return new ProducerClient(i);
    }

    Client createConsumer(int i, boolean drain) {
        return new ConsumerClient(i, drain);
    }

    abstract protected Destination destination(int i) throws Exception;

    String indexedDestinationName(int i) {
        if ("queue".equals(destinationType)) {
            return queuePrefix + getDestinationName() + "-" + (i % destinationCount);
        }
        if ("topic".equals(destinationType)) {
            return topicPrefix + getDestinationName() + "-" + (i % destinationCount);
        }
        throw new RuntimeException("Unsupported destination type: " + destinationType);
    }

    abstract protected ConnectionFactory factory() throws Exception;

    int jmsAckMode() {
        if ("auto".equals(ackMode)) {
            return Session.AUTO_ACKNOWLEDGE;
        }
        if ("client".equals(ackMode)) {
            return Session.CLIENT_ACKNOWLEDGE;
        }
        if ("dups_ok".equals(ackMode)) {
            return Session.DUPS_OK_ACKNOWLEDGE;
        }
        if ("transacted".equals(ackMode)) {
            return Session.SESSION_TRANSACTED;
        }
        throw new RuntimeException("Invalid ack mode: " + ackMode);
    }

    void loadStartRendezvous(JMSClient client, Session session) throws InterruptedException {
        if (connectedLatch != null) {
            connectedLatch.countDown();
        }
        if (applyLoadLatch != null) {
            applyLoadLatch.await();
        }
    }

    abstract class JMSClient implements Client {

        volatile
        Connection connection;
        long messageCounter = 0L;
        AtomicBoolean done = new AtomicBoolean();

        Thread worker;

        Thread closeThread = null;

        synchronized void dispose() {
            if (this.connection != null) {
                final Connection connection = this.connection;
                closeThread = new Thread(name() + " closer") {
                    @Override
                    public void run() {
                        try {
                            connection.close();
                        } catch (Throwable e) {
                        }
                    }
                };
                closeThread.start();
                this.connection = null;
            }
        }

        abstract void execute() throws JMSException, InterruptedException, Exception;

        public void start() {
            worker = new Thread(name + " worker") {
                @Override
                public void run() {
                    int reconnectDelay = 0;
                    while (!done.get()) {
                        try {

                            if (reconnectDelay != 0) {
                                Thread.sleep(reconnectDelay);
                                reconnectDelay = 0;
                            }

                            // Don't reconnect until we finish disconnecting..
                            if (closeThread != null) {
                                closeThread.join();
                                closeThread = null;
                            }

                            connection = factory().createConnection(userName, password);
                            if (durable) {
                                connection.setClientID(name);
                            }
                            connection.setExceptionListener((e) -> {
                            });
                            connection.start();

                            execute();

                        } catch (Throwable e) {
                            if (!done.get()) {
                                if (displayErrors) {
                                    e.printStackTrace();
                                }
                                errorCounter.incrementAndGet();
                                reconnectDelay = 1000;
                            }
                        } finally {
                            dispose();
                        }
                    }
                }
            };
            worker.start();
        }

        public void shutdown() {
            done.set(true);
            dispose();
        }

        public void waitForShutdown() throws InterruptedException {
            if (worker != null) {
                long start = System.currentTimeMillis();
                worker.join(6000);
                while (worker.isAlive()) {
                    if (allowWorkerInterrupt) {
                        System.out.println(name + " - worker did not shutdown quickly.. interrupting thread.");
                        worker.interrupt();
                        worker.join(1000);
                    } else {
                        System.out.println(name + " - worker did not shutdown quickly...");
                        worker.join(1000);
                    }
                }
                if (closeThread != null) {
                    long wait = 6000 - (System.currentTimeMillis() - start);
                    if (wait > 0) {
                        closeThread.join(wait);
                    }
                    while (closeThread.isAlive()) {
                        if (allowWorkerInterrupt) {
                            System.out.println(name + " closing thread did not shutdown quickly.. interrupting thread.");
                            closeThread.interrupt();
                            closeThread.join(1000);
                        } else {
                            System.out.println(name + " closing thread did not shutdown quickly...");
                            closeThread.join(1000);
                        }
                    }
                    closeThread.join();
                }
                worker = null;
            }
        }

        abstract String name();
    }

    class ConsumerClient extends JMSClient {
        private final int id;
        private final boolean drain;

        @Override
        public int id() {
            return id;
        }

        public String name() {
            return "consumer " + id;
        }

        public ConsumerClient(int id, boolean drain) {
            this.id = id;
            this.drain = drain;
        }

        public void execute() throws Exception {

            Session session = txSize == 0 ?
                    connection.createSession(false, jmsAckMode()) :
                    connection.createSession(true, Session.SESSION_TRANSACTED);

            // Draining a durable topic is easy.. just unsubscribe it!
            if (drain && durable) {
                loadStartRendezvous(this, session);
                session.unsubscribe(name);
                done.set(true);
                return;
            }

            MessageConsumer consumer = durable ?
                    session.createDurableSubscriber((Topic) destination(id), name, selector, noLocal)
                    :
                    session.createConsumer(destination(id), selector, noLocal);

            loadStartRendezvous(this, session);

            final int txCounter[] = new int[]{0};
            MessageListener listener = (msg) -> {
                try {
                    long latency = System.nanoTime() - msg.getLongProperty("ts");
                    updateMaxLatency(latency);
                    consumerCounter.incrementAndGet();
                    consumerSleep.apply(this);
                    if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
                        msg.acknowledge();
                    }

                    if (txSize != 0) {
                        txCounter[0] += 1;
                        if (txCounter[0] == txSize) {
                            session.commit();
                            txCounter[0] = 0;
                        }
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            };

            if (useMessageListener) {
                consumer.setMessageListener(listener);
                while (!done.get()) {
                    Thread.sleep(500);
                }
            } else {
                while (!done.get()) {
                    Message msg = consumer.receive(500);
                    if (msg != null) {
                        listener.onMessage(msg);
                    } else {
                        // commit early once we don't get anymore messages.
                        if (txSize != 0 && txCounter[0] > 0) {
                            session.commit();
                            txCounter[0] = 0;
                        }
                    }
                }
            }
            consumer.close();
        }

    }

    class ProducerClient extends JMSClient {

        private final int id;

        ProducerClient(int id) {
            this.id = id;
        }

        @Override
        public int id() {
            return id;
        }

        public String name() {
            return "producer " + id;
        }

        public void execute() throws Exception {

            Session session = txSize == 0 ?
                    connection.createSession(false, jmsAckMode())
                    :
                    connection.createSession(true, Session.SESSION_TRANSACTED);

            MessageProducer producer = session.createProducer(destination(id));
            producer.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

            TextMessage msg = session.createTextMessage(body(name()));
            headersFor(id).forEach(x -> {
                try {
                    msg.setStringProperty(x.getKey(), x.getValue());
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            });

            loadStartRendezvous(this, session);

            final int txCounter[] = new int[]{0};
            while (!done.get()) {
                msg.setLongProperty("ts", System.nanoTime());
                sendMessage(producer, msg);
                producerCounter.incrementAndGet();

                producerSleep.apply(this);
                if (txSize != 0) {
                    txCounter[0] += 1;
                    if (txCounter[0] == txSize) {
                        session.commit();
                        txCounter[0] = 0;
                    }
                }
            }

        }
    }

    void sendMessage(MessageProducer producer, TextMessage msg) throws Exception {
        producer.send(msg);
    }

    String body(String name)
    {
        StringBuffer buffer = new StringBuffer(messageSize);
        buffer.append("Message from " + name + "\n");
        for (int i = buffer.length(); i < messageSize; i++) {
            buffer.append('a' + (i % 26));
        }
        String rc = buffer.toString();
        if (rc.length() > messageSize) {
            return rc.substring(0, messageSize);
        } else {
            return rc;
        }
    }


}
