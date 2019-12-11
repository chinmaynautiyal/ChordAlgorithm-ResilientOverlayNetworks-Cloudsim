
package com.nautiyalraj

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import akka.actor.ActorSystem
import akka.util.Timeout
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.TextMessage
import akka.stream.scaladsl._
import akka.stream.{ActorAttributes, ActorMaterializer, OverflowStrategy, Supervision}




import scala.concurrent.Await
import scala.concurrent.duration._

/*
object chordDriver extends LazyLogging{


  /**
   * This is the entry point of the chord simulation.
   *
   *
   */
  def main(args: Array[String] ): Unit = {


    //take parameters for the simulation from the config files.


    //input configuration parameters

    val config = ConfigFactory.load()

    val numNodes = 5 //numbder of nodes to be simulated in the overlay network
    val numUsers = 2 //number of actors that can send requests

    val maxRequests = config.getString("maxReq")
    val minRequests = config.getString( " minRequests")

    val timeStampList = config.getStringList("checkpoints")


    val readWrtieRatio = 1 //what is this used for?

    val simDuration = config.getString("duration")  //test if this is within start and end sim for simulation
    //implicit vals

    implicit val system = ActorSystem("chordServices")
    implicit val materializer = ActorMaterializer()
    implicit val ec = system.dispatcher

    var hashBits = 8 * (((((math.log(numNodes) / math.log(2)) / 8).toFloat).ceil).toInt) //hashed key bits
    implicit val timeout: Timeout = 3.seconds

    val masterNode =
      system.actorOf(masterNode.props(hashBits), "Governor")

    //actor which logs events published by nodes

    val eventWriter = system.actorOf(EventWriter.props, "EventWriter")
    system.eventStream.subscribe(eventWriter, classOf[event]) //implement event










  }


}
*/