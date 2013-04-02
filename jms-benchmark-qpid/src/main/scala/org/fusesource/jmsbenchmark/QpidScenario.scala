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
import org.apache.qpid.client._
import org.apache.qpid.jms.ConnectionURL

object QpidScenario {
  def main(args:Array[String]):Unit = {
    val scenario = new QpidScenario
    scenario.url = "tcp://localhost:5672"
    scenario.user_name = "guest"
    scenario.password = "guest"
    scenario.display_errors = true
    scenario.message_size = 20
    scenario.destination_type = "queue"
    scenario.consumers = 1
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
class QpidScenario extends JMSClientScenario {

  override protected def factory:ConnectionFactory = {
    val client_id = user_name
    val virtual_host = ""
    val x = new AMQConnectionURL(url)
    x.setClientName(client_id)
    x.setVirtualHost(virtual_host)
    new AMQConnectionFactory(x)
  }

  override protected def destination(i:Int):Destination = destination_type match {
    case "queue" =>
      new AMQQueue("BURL:direct://amq.direct//qload-"+i)
    case "topic" =>
      new AMQTopic("BURL:topic://amq.topic/tload"+i+"/sub")
    case _ =>
      sys.error("Unsuported destination type: "+destination_type)
  }

}