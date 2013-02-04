/**
 * Copyright (C) 2009-2011 the original author or authors.
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
package org.fusesource.jmsbenchmark

import javax.jms.{Destination, ConnectionFactory}
import org.apache.qpid.amqp_1_0.jms.impl.{TopicImpl, ConnectionFactoryImpl, QueueImpl}
import java.net.URI

object Qpid10Scenario {
  def main(args:Array[String]):Unit = {
    val scenario = new Qpid10Scenario
    scenario.url = "tcp://localhost:61613"
    scenario.user_name = "admin"
    scenario.password = "password"
    scenario.display_errors = true
    scenario.message_size = 20
    scenario.destination_type = "queue"
    scenario.consumers = 1
    scenario.queue_prefix = "queue://"
    scenario.topic_prefix = "topic://"
    scenario.run()
  }
}

/**
 * <p>
 * Qpid implementation of the JMS Scenario class.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Qpid10Scenario extends JMSClientScenario {

  override protected def factory:ConnectionFactory = {
    val client_id = user_name
    val virtual_host = ""
    val u = new URI(url)
    var host = u.getHost
    var port = u.getPort
    if ( port == -1 ) {
      port = 5672
    }
    var ssl = false
    if ( u.getScheme == "amqps" ) {
      ssl = true
    }
    new ConnectionFactoryImpl(u.getHost, u.getPort, user_name, password, client_id, virtual_host, ssl)
  }

  override protected def destination(i:Int):Destination = destination_type match {
    case "queue" => new QueueImpl(indexed_destination_name(i))
    case "topic" => new TopicImpl(indexed_destination_name(i))
    case _ =>
      error("Unsuported destination type: "+destination_type)
  }

}