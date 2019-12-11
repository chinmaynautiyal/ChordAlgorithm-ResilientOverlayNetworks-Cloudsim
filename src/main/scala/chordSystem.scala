package com.nautiyalraj

import java.security.MessageDigest

import chord.node
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.commons.codec.binary.Hex

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}
import scala.concurrent.ExecutionContext.Implicits.global

class chordSystem(totalnodes: Int, NumRequests: Int, users: Int) extends Actor {
  import chordSystem._
  val numUsers = users
  var hops = 0
  var requestsProcessed = 0
  var numNodes = totalnodes
  var nameStrings = new ArrayBuffer[String]()
  var nodes = new ArrayBuffer[ActorRef]()
  var m = 0
  var indexMap = new mutable.HashMap[Int, Int]()

  def compareMod(s1: ActorRef, s2: ActorRef) = {
    (Integer.parseInt(s1.path.name)) < (Integer.parseInt(s2.path.name))
  }

  def makeFingertable(nodes: ArrayBuffer[ActorRef], numNodes: Int) = {
    val sortedNodes = nodes.sortWith(compareMod)
    //sortedNodes

    for (i <- 0 to numNodes - 1) {

      var fingerTableMap = SortedMap[Int, ActorRef]()
      var k = Integer.parseInt(sortedNodes(i).path.name)
      var n = (k) //% math.pow(2,numNodes).toInt).toInt
      var l = 1
      //fill n's fingertable
      while (l <= m) {

        var flag = true
        // println("n->"+n)
        var entry = ((n + math.pow(2, l - 1)) % (math.pow(2, m).toInt)).toInt //sharique wrong
        var min = 99999
        var min_id = 0
        var count = 0
        var r = 0

        if (indexMap.contains(entry)) {
          r = indexMap(entry)
        } else {
          entry = (entry + 1) % (math.pow(2, m).toInt)
          while (indexMap.contains(entry) != true) {
            entry = (entry + 1) % (math.pow(2, m).toInt)
          }

          r = indexMap(entry)
        }

        fingerTableMap += (l -> sortedNodes(r))

        l = l + 1

      }

      //nodes(i)! fingerTable(fingerTableMap)
      val fingerTableSortedMap = SortedMap.empty[Int, ActorRef] ++ fingerTableMap
      //fingerTableMap= mutable.Map.empty[Int,ActorRef]

      // fingerTableMap ++=  fingerTableSortedMap
      println("fingertable for node " + sortedNodes(i).path.name + " -> " + fingerTableSortedMap)
      sortedNodes(i) ! setFingerTable(fingerTableSortedMap)

    }

  }


  def receive = {


    //increasing the messages received when running the simulation
    case increment(count: Int) => {
      requestsProcessed += 1
      hops += count
      println("requestsProcessed = " + requestsProcessed)
      if (requestsProcessed >= numNodes * NumRequests) {
        println("Total hops for " + numNodes * NumRequests + " requests are " + hops + ", Average hops = " + (hops / (numNodes * NumRequests).toFloat))

        context.system.terminate()
      }

    }


    //starting the chord system
    case "start" => {
      m = 8 * (((((math.log(numNodes) / math.log(2)) / 8).toFloat).ceil).toInt)
      for (i <- 0 to numNodes - 1) {
        var st: String = hashing.hashKey("node " + i)
        var name = Integer.parseInt(st, 16).toString
        nameStrings += name
      }
      nameStrings = nameStrings.distinct


      while (nameStrings.length < numNodes) {
        var st: String = hashing.hashKey("node " + Random.nextInt(10000000))
        var name = Integer.parseInt(st, 16).toString
        if (nameStrings.contains(name) == false)
          nameStrings += name
      }
      println("creating nodes m = " + m + " numNodes = " + numNodes)
      for (i <- 0 to numNodes - 1) {
        var st: String = hashing.hashKey("node " + i)
        nodes += context.actorOf(Props(new node(st, numNodes)), name = nameStrings(i))
        println(Integer.parseInt(st, 16).toString)
      }

      val sortedNodes = nodes.sortWith(compareMod)

      nodes = sortedNodes

      for (i <- 0 to numNodes - 1) {
        indexMap += Integer.parseInt(nodes(i).path.name) -> i
      }

      sortedNodes(0) ! setNeighbours(sortedNodes(numNodes - 1), sortedNodes(1))
      for (i <- 1 to numNodes - 2)
        sortedNodes(i) ! setNeighbours(sortedNodes(i - 1), sortedNodes(i + 1))
      sortedNodes(numNodes - 1) ! setNeighbours(sortedNodes(numNodes - 2), sortedNodes(0))

      makeFingertable(nodes, numNodes)
      //node joins the network
      var newNodeName: String = sortedNodes(0).path.name
      var newNode: ActorRef = null
      breakable{
        while (true) {
          var str: String = hashing.hashKey("Node " + Random.nextInt(10000000))
          newNodeName = Integer.parseInt(str, 16).toString
          if (nameStrings.contains(newNodeName) == false)
          {
            nameStrings += newNodeName
            newNode = context.actorOf(Props(new node(newNodeName, numNodes)), name = newNodeName)
            break;
          }
        }
      }
      println("node to join->" + newNode)
      newNode ! nodeJoin(sortedNodes(0))

      nodes = sortedNodes

    }

    case "networkReady" => {
      nodes += sender
      numNodes += 1
      //sending message to ith node to start job
      for (i <- 0 to numNodes - 1) {
        nodes(i) ! startjob(NumRequests)
      }
    }


  }
}

object chordSystem{

  //insert messages to be passed here
  case class setNeighbours(p: ActorRef, s: ActorRef)
  case class setFingerTable(map: SortedMap[Int, ActorRef])
  case class nodeJoin(existingNode: ActorRef)
  case class startjob(numOfRequests: Int)
  case class increment(count: Int)

  def main(args: Array[String]) : Unit = {
    val config = ConfigFactory.load()

    val numNodes = config.getString("numNodes") //numbder of nodes to be simulated in the overlay network
    val numUsers = config.getString("numUsers") //number of actors that can send requests

    //val maxRequests = config.getString("maxReq")
    //val minRequests = config.getString( " minRequests")
    val request = config.getString("numRequests")
    val timeStampList = config.getStringList("checkpoints")


    val readWrtieRatio = 1
    val system = ActorSystem("ChordSimulator")

    val args0 = 10
    val mainSystem = system.actorOf(Props(new chordSystem(10, 2, numUsers.toInt)), name = "Chord")
    //initiating system
    mainSystem ! "start"

    }

  }


