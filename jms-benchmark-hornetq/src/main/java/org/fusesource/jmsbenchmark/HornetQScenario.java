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

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.jms.client.HornetQConnectionFactory;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import java.net.URI;
import java.util.HashMap;

public class HornetQScenario extends JMSClientScenario {

    @Override
    protected ConnectionFactory factory() throws Exception {
        URI u = new URI(url);

        HashMap<String, Object> options = new HashMap<String, Object>();
        options.put("host", u.getHost());
        options.put("port", u.getPort());

        TransportConfiguration transportConfiguration = new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory", options);

        HornetQConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration);
        cf.setRetryInterval(100);
        cf.setReconnectAttempts(10);
        return (ConnectionFactory) cf;
    }

    @Override
    protected Destination destination(int i) {
        if ("queue".equals(destinationType)) {
            return HornetQJMSClient.createQueue(indexedDestinationName(i));
        }
        if ("topic".equals(destinationType)) {
            return HornetQJMSClient.createTopic(indexedDestinationName(i));
        }
        throw new RuntimeException("Unsupported destination type: " + destinationType);
    }
}
