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

import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQTopic;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import java.net.URISyntaxException;

public class QpidScenario extends JMSClientScenario {

    @Override
    protected ConnectionFactory factory() throws Exception {
        String clientId = userName;
        String virtualHost = "";
        AMQConnectionURL x = new AMQConnectionURL(url);
        x.setClientName(clientId);
        x.setVirtualHost(virtualHost);
        return new AMQConnectionFactory(x);
    }

    @Override
    protected Destination destination(int i) throws URISyntaxException {
        if ("queue".equals(destinationType)) {
            return new AMQQueue(indexedDestinationName(i) + ";{create:always,node:{durable:true}}");
        }
        if ("topic".equals(destinationType)) {
            return new AMQTopic("amq.topic/" + indexedDestinationName(i));
        }
        throw new RuntimeException("Unsupported destination type: " + destinationType);

    }
}
