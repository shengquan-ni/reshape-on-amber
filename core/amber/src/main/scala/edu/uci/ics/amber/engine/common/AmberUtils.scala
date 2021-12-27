package edu.uci.ics.amber.engine.common

import java.io.{BufferedReader, InputStreamReader}
import java.net.{InetAddress, URL}
import edu.uci.ics.amber.clustering.ClusterListener
import akka.actor.{ActorSystem, DeadLetter, Props}
import com.typesafe.config.ConfigFactory
import edu.uci.ics.amber.engine.architecture.messaginglayer.DeadLetterMonitorActor

import scala.collection.mutable.ArrayBuffer

object AmberUtils {

  def mean(workloads: ArrayBuffer[Long]): Double = {
    var mean: Double = 0
    workloads.foreach(load => { mean = mean + load })
    mean = mean / workloads.size
    mean
  }

  def sampleMeanError(workloads: ArrayBuffer[Long]): Double = {
    if (workloads.size <= 1) {
      return Double.MaxValue
    }
    var sum: Double = 0
    val meanOfLoads: Double = mean(workloads)
    workloads.foreach(load => {
      sum = sum + (load - meanOfLoads) * (load - meanOfLoads)
    })
    val ssd = scala.math.sqrt(sum / (workloads.size - 1))
    val error = ssd * (1 + (1.0 / (2.0 * workloads.size)))
    error
  }

  def reverseMultimap[T1, T2](map: Map[T1, Set[T2]]): Map[T2, Set[T1]] =
    map.toSeq
      .flatMap { case (k, vs) => vs.map((_, k)) }
      .groupBy(_._1)
      .mapValues(_.map(_._2).toSet)

  def startActorMaster(localhost: Boolean): ActorSystem = {
    var localIpAddress = "localhost"
    if (!localhost) {
      if (!Constants.gcpExp) {
        try {
          val query = new URL("http://checkip.amazonaws.com")
          val in = new BufferedReader(new InputStreamReader(query.openStream()))
          localIpAddress = in.readLine()
        } catch {
          case e: Exception => throw e
        }
      } else {
        val localhost: InetAddress = InetAddress.getLocalHost
        localIpAddress = localhost.getHostAddress
      }
    }

    val config = ConfigFactory
      .parseString(s"""
        akka.remote.artery.canonical.port = 2552
        akka.remote.artery.canonical.hostname = $localIpAddress
        akka.cluster.seed-nodes = [ "akka://Amber@$localIpAddress:2552" ]
        akka.actor.serialization-bindings."java.lang.Throwable" = akka-misc
        """)
      .withFallback(ConfigFactory.load("clustered"))

    val system = ActorSystem("Amber", config)
    val info = system.actorOf(Props[ClusterListener], "cluster-info")
    val deadLetterMonitorActor =
      system.actorOf(Props[DeadLetterMonitorActor], name = "dead-letter-monitor-actor")
    system.eventStream.subscribe(deadLetterMonitorActor, classOf[DeadLetter])

    system
  }

  def startActorWorker(mainNodeAddress: Option[String]): ActorSystem = {
    val addr = mainNodeAddress.getOrElse("localhost")
    var localIpAddress = "localhost"
    if (!mainNodeAddress.isEmpty) {
      if (!Constants.gcpExp) {
        try {
          val query = new URL("http://checkip.amazonaws.com")
          val in = new BufferedReader(new InputStreamReader(query.openStream()))
          localIpAddress = in.readLine()
        } catch {
          case e: Exception => throw e
        }
      } else {
        val localhost: InetAddress = InetAddress.getLocalHost
        localIpAddress = localhost.getHostAddress
      }
    }
    val config = ConfigFactory
      .parseString(s"""
        akka.remote.artery.canonical.hostname = $localIpAddress
        akka.remote.artery.canonical.port = 0
        akka.cluster.seed-nodes = [ "akka://Amber@$addr:2552" ]
        akka.actor.serialization-bindings."java.lang.Throwable" = akka-misc
        """)
      .withFallback(ConfigFactory.load("clustered"))
    val system = ActorSystem("Amber", config)
    val info = system.actorOf(Props[ClusterListener], "cluster-info")
    val deadLetterMonitorActor =
      system.actorOf(Props[DeadLetterMonitorActor], name = "dead-letter-monitor-actor")
    system.eventStream.subscribe(deadLetterMonitorActor, classOf[DeadLetter])
    Constants.masterNodeAddr = addr
    system
  }
}
