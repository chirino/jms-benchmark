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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.AsyncCallback;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.*;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import java.lang.management.ManagementFactory;
import java.util.concurrent.Semaphore;

public class ActiveMQScenario extends JMSClientScenario {

    boolean jmsBypass = java.lang.Boolean.getBoolean("jms_bypass");

    @Override
    protected ConnectionFactory factory() throws Exception {
        ActiveMQConnectionFactory rc = new ActiveMQConnectionFactory();
        rc.setBrokerURL(url);
        rc.setCloseTimeout(3 * 1000);

        // Lets optimize the prefetch used for the scenario.
        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        CompositeData heapUsage = (CompositeData) mbeanServer.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage");
        long heapMax = ((Long) heapUsage.get("max")).longValue();

        long prefetchAvailableHeap = heapMax / 10; // use 1/10 of the heap for prefetch.
        if (prefetchAvailableHeap > 0 && consumers * messageSize > 0) {
            long prefechSize = prefetchAvailableHeap / (consumers * messageSize);
            rc.getPrefetchPolicy().setAll((int) Math.min(prefechSize, 100 * 1000));
        }

        sendSemaphore = new Semaphore((int) Math.min(prefetchAvailableHeap / (producers * messageSize), Integer.MAX_VALUE));

        rc.setWatchTopicAdvisories(false);
        ackMode = "client";

        if (jmsBypass) {
            useMessageListener = true;
            rc.getPrefetchPolicy().setAll(1000 * 100);
            rc.setAlwaysSessionAsync(false);
            rc.setCheckForDuplicates(false);
        }

        return rc;
    }


    Semaphore sendSemaphore = null;

    AsyncCallback sendCallback = new AsyncCallback() {
        public void onException(JMSException error) {
            if (!done.get() && displayErrors) {
                error.printStackTrace();
            }
            sendSemaphore.release();
        }

        public void onSuccess() {
            // An app could use this to know if the send succeeded.
            sendSemaphore.release();
        }
    };

    @Override
    void sendMessage(MessageProducer producer, TextMessage msg) throws Exception {
        if (jmsBypass && persistent && txSize == 0) {
            sendSemaphore.acquire();
            ((ActiveMQMessageProducer) producer).send(msg, sendCallback);
        } else {
            super.sendMessage(producer, msg);
        }
    }


    @Override
    protected Destination destination(int i) {
        if ("queue".equals(destinationType)) {
            return new ActiveMQQueue(indexedDestinationName(i));
        }
        if ("topic".equals(destinationType)) {
            return new ActiveMQTopic(indexedDestinationName(i));
        }
        throw new RuntimeException("Unsupported destination type: " + destinationType);
    }

}