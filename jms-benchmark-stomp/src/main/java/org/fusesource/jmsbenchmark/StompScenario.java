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

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsQueue;
import org.fusesource.stomp.jms.StompJmsTopic;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

public class StompScenario extends JMSClientScenario {

    @Override
    protected ConnectionFactory factory() throws Exception {
        allowWorkerInterrupt = true;
        StompJmsConnectionFactory rc = new StompJmsConnectionFactory();
        rc.setBrokerURI(url);
        rc.setOmitHost(true);
        rc.setDisconnectTimeout(1500);
        return rc;
    }

    @Override
    protected Destination destination(int i) throws Exception {
        if ("queue".equals(destinationType)) {
            return new StompJmsQueue("/queue/", indexedDestinationName(i));
        }
        if ("topic".equals(destinationType)) {
            return new StompJmsTopic("/topic/", indexedDestinationName(i));
        }
        throw new RuntimeException("Unsupported destination type: " + destinationType);
    }
}
