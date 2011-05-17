package org.fusesource.jmsbenchmark

import javax.jms.{Destination, ConnectionFactory}
import org.fusesource.stompjms.{StompJmsTopic, StompJmsQueue, StompJmsConnectionFactory}

/**
 * <p>
 * ActiveMQ implementation of the JMS Scenario class.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StompScenario extends JMSClientScenario {

  override protected def factory:ConnectionFactory = {
    val rc = new StompJmsConnectionFactory
    rc.setBrokerURI(url)
    rc
  }

  override protected def destination(i:Int):Destination = destination_type match {
    case "queue" => new StompJmsQueue(indexed_destination_name(i))
    case "topic" => new StompJmsTopic(indexed_destination_name(i))
    case _ => sys.error("Unsuported destination type: "+destination_type)
  }

}