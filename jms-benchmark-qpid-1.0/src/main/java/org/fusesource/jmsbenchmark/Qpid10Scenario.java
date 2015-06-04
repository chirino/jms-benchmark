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

import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.apache.qpid.amqp_1_0.jms.impl.TopicImpl;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import java.net.URI;

public class Qpid10Scenario extends JMSClientScenario {

    @Override
    protected ConnectionFactory factory() throws Exception {
        String clientId = userName;
        String virtualHost = "";
        URI u = new URI(url);
        String host = u.getHost();
        int port = u.getPort();
        if (port == -1) {
            port = 5672;
        }
        boolean ssl = false;
        if ("amqps".equals(u.getScheme())) {
            ssl = true;
        }
        return new ConnectionFactoryImpl(u.getHost(), port, userName, password, clientId, virtualHost, ssl);
    }

    @Override
    protected Destination destination(int i) throws Exception {
        if ("queue".equals(destinationType)) {
            return new QueueImpl(indexedDestinationName(i));
        }
        if ("topic".equals(destinationType)) {
            return new TopicImpl(indexedDestinationName(i));
        }
        throw new RuntimeException("Unsupported destination type: " + destinationType);
    }
}
