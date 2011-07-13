package org.fusesource.jmsbenchmark

import javax.jms.{Destination, ConnectionFactory}
import org.hornetq.api.core.TransportConfiguration
import org.hornetq.api.jms.{JMSFactoryType, HornetQJMSClient}
import java.util.HashMap
import java.net.URI

/**
 * <p>
 * HornetQ implementation of the JMS Scenario class.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class HornetQScenario extends JMSClientScenario {

  override protected def factory:ConnectionFactory = {
    val u = new URI(url)

    val options = new HashMap[String, Object]()
    options.put("host", u.getHost)
    options.put("port", u.getPort.asInstanceOf[AnyRef])

    val transportConfiguration = new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory", options);
    val cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,transportConfiguration);
    cf.asInstanceOf[ConnectionFactory]
  }

  override protected def destination(i:Int):Destination = destination_type match {
    case "queue" => HornetQJMSClient.createQueue(indexed_destination_name(i))
    case "topic" => HornetQJMSClient.createTopic(indexed_destination_name(i))
    case _ => error("Unsuported destination type: "+destination_type)
  }

}