package com.nautiyalraj.chord

import java.security.MessageDigest
import java.util.concurrent.TimeUnit


import akka.actor.{Actor, ActorRef, Cancellable}
import org.apache.commons.codec.binary.Hex

import com.nautiyalraj.hashing
import scala.collection.immutable.SortedMap
import scala.concurrent.duration.Duration
import scala.util.Random
import scala.concurrent._
import ExecutionContext.Implicits.global

/**
 *
 *
 *
 * @param id identifier for node
 * @param numNodes number of nodes to be simulated with the chord network
 */

class node(id: String, numNodes: Int) extends Actor{
  import node._
  val num = numNodes
  val nodeID = id


  //finger table map for each node
  var fingerTable  = SortedMap.empty[Int, ActorRef]

  //predecessors and successor initialisation for each node
  var pred: ActorRef = null
  var succ: ActorRef = null

  //counting requests performed when looking up
  var requestCounter = 0
  var hopCount = 0

  //Actor reference to server of request
  var server: ActorRef = null
  var tick: Cancellable = null
  var m = 8 * (((((math.log(numNodes) / math.log(2)) / 8).toFloat).ceil).toInt)


  def receive = {
    case generateRequests(numOfRequests) => {
      if (requestCounter == numOfRequests) {
        tick.cancel()
      } else {

        requestCounter += 1

        var file = Integer.parseInt(hashing.hashKey("file" + requestCounter + Random.nextInt(10000000)), 16)
        println("Search started at " + Integer.parseInt(self.path.name) + " for " + file)
        self ! findFileWithHop(Integer.parseInt(self.path.name), file, 0)

      }


    }


    case startjob(numOfRequests) => {

      server = sender
      tick = context.system.scheduler.schedule(Duration.create(10, TimeUnit.MILLISECONDS), Duration.create(1000, TimeUnit.MILLISECONDS), self, generateRequests(numOfRequests))

    }

      //update others case started
    case updateOthers(initiator, p, s) => {
      var successor = Integer.parseInt(s.path.name)
      var predecessor = Integer.parseInt(p.path.name)
      var newNodeId = Integer.parseInt(initiator.path.name)
      println(s"$predecessor $newNodeId  $successor")
      //println(predecessor + " " + newNodeId + " " + successor)
      if (!initiator.equals(self)) {
        var entry: Int = 0
        var newNodeID = Integer.parseInt(initiator.path.name)
        for ((key, value) <- fingerTable) {
          if (value.equals(s)) {
            entry = (((Integer.parseInt(self.path.name) + math.pow(2, key - 1).toInt)) % (math.pow(2, m).toInt)).toInt
            if(predecessor < successor){
              if (newNodeId < successor) {
                if (entry <= newNodeId) {
                  var fingerTableMap = scala.collection.mutable.Map[Int, ActorRef]() ++= fingerTable
                  fingerTableMap(key) = initiator
                  fingerTable = SortedMap.empty[Int, ActorRef] ++ fingerTableMap

                }
              }
            }
            else{
              if(newNodeId>=0 && newNodeId < successor){
                if(entry > predecessor){
                  var fingerTableMap = scala.collection.mutable.Map[Int, ActorRef]() ++= fingerTable
                  fingerTableMap(key) = initiator
                  fingerTable = SortedMap.empty[Int, ActorRef] ++ fingerTableMap
                }
                else if(entry<=newNodeID){
                  var fingerTableMap = scala.collection.mutable.Map[Int, ActorRef]() ++= fingerTable
                  fingerTableMap(key) = initiator
                  fingerTable = SortedMap.empty[Int, ActorRef] ++ fingerTableMap
                }
              }
              else{
                if(entry <= newNodeID && entry > predecessor){
                  var fingerTableMap = scala.collection.mutable.Map[Int, ActorRef]() ++= fingerTable
                  fingerTableMap(key) = initiator
                  fingerTable = SortedMap.empty[Int, ActorRef] ++ fingerTableMap
                }
              }
            }

          }


        }
        println("updated finger table for " + self + " node->" + fingerTable)
        succ ! updateOthers(initiator, p, s)
      } else {
        println("all finger tables updated")
        server ! "networkReady"
      }
    }
      //update others ends


      //update finger table
    case updateFingerTable(pos, node) => {

      fingerTable += (pos -> node)
      if (fingerTable.size == m) {
        pred ! fixSuccessor(self)
        succ ! fixPredecessor(self)

      }
    }

    case fixSuccessor(nodes) => {
        succ = nodes
    }


    case fixPredecessor(nodes) => {
        var oldPred = pred
        pred = nodes
        self ! updateOthers(nodes, oldPred, self)
    }



    case findFileWithHop(initiator, k, cnt) => {
        var count = cnt
        var flag = false
        if (k <= Integer.parseInt(self.path.name)) {
          if (k == Integer.parseInt(self.path.name)) {
            println("file request from " + initiator + ": key= " + k + " found in " + self.path.name)
            flag = true;
            server ! increment(count)
          } else if (k > Integer.parseInt(pred.path.name)) {
            println("file request from " + initiator + ": key= " + k + " found in " + self.path.name)
            flag = true;
            server ! increment(count)
            // count=count+1
          } else if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name))) {
            println("file request from " + initiator + ": key= " + k + " found in " + self.path.name)
            flag = true;
            server ! increment(count)
            // count=count+1
          }
        }

        if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name)) && k > Integer.parseInt(self.path.name) && k > Integer.parseInt(pred.path.name)) {
          println("file request from " + initiator + ": key= " + k + " found in " + self.path.name)
          flag = true;
          server ! increment(count)
        }

        var minIndex = 0
        var min = k - (Integer.parseInt(self.path.name))
        if(min < 0)
          min += (math.pow(2, m).toInt)
        if (!flag) {
          for ((key, value) <- fingerTable) {
            var x = k - (Integer.parseInt(value.path.name)).toInt
            if (x < 0)
              x += (math.pow(2, m).toInt)
            if (x < min) {
              min = x
              minIndex = key
            }
          }

          if (minIndex != 0) {
            flag = true
            count += 1
            fingerTable(minIndex) ! findFileWithHop(initiator, k, count)
          }
        }

        if (!flag) {

          count += 1
          succ ! findFileWithHop(initiator, k, count)

        }
      }

      //find successor case
    case findSuccessor(k, initiator) => {
      //var count = cnt

      var flag = false

      if (k <= Integer.parseInt(self.path.name)) {
        if (k == Integer.parseInt(self.path.name)) {

          flag = true;
          initiator ! notifySuccessorPredecessor(self, pred)

        } else if (k > Integer.parseInt(pred.path.name)) {

          flag = true;
          initiator ! notifySuccessorPredecessor(self, pred)

        } else if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name))) {

          flag = true;
          initiator ! notifySuccessorPredecessor(self, pred)
          // count=count+1
        }
      }

      if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name)) && k > Integer.parseInt(self.path.name) && k > Integer.parseInt(pred.path.name)) {

        flag = true;
        initiator ! notifySuccessorPredecessor(self, pred)
      }

      var minIndex = 0
      var min = k - (Integer.parseInt(self.path.name))
      if (!flag) {
        for ((key, value) <- fingerTable) {
          var x = k - (Integer.parseInt(value.path.name)).toInt
          if (x < 0)
            x += (math.pow(2, m).toInt)
          if (x < min) {
            min = x
            minIndex = key
          }
        }

        if (minIndex != 0) {
          flag = true

          fingerTable(minIndex) ! findSuccessor(k, initiator)
        }
      }

      if (!flag) {
        succ ! findSuccessor(k, initiator)

      }
    }



    case setNeighbours(p, s) => {
      this.pred = p
      this.succ = s
      //println("Neighbours of " + Integer.parseInt(self.path.name) + " = " + Integer.parseInt(pred.path.name) + ", " + Integer.parseInt(succ.path.name))
    }

    case setFingerTable(fingTable) => {
      fingerTable = fingTable
      //println("fingertable set:" + self.path.name + "fingertable->" + fingerTable)
    }
    case notifySuccessorPredecessor(s, p) => {

      succ = s
      pred = p
      //println("successor" + succ + "predecessor" + pred)
      self ! "init_fingerTable"

    }
    case nodeJoin(existingNode) => {
      server = sender
      existingNode ! findSuccessor(Integer.parseInt(self.path.name), self)

    }


    case "init_fingerTable" => {
      var n = Integer.parseInt(self.path.name)
      for (l <- 1 to m) {
        var entry = ((n + math.pow(2, l - 1)) % (math.pow(2, m).toInt)).toInt //sharique wrong
        succ ! findfingerTableSuccessor(entry, l, self)

      }

    }

    case findfingerTableSuccessor(k, pos, initiator) => {
      //var count = cnt

      var flag = false

      if (k <= Integer.parseInt(self.path.name)) {
        if (k == Integer.parseInt(self.path.name)) {
          //println("Successor found at->" + self.path.name)
          flag = true;
          initiator ! updateFingerTable(pos, self)
          // count=count+1
        } else if (k > Integer.parseInt(pred.path.name)) {
          //println("Successor found at->" + self.path.name)
          flag = true;
          initiator ! updateFingerTable(pos, self)
          // count=count+1
        } else if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name))) {
          //println("Successor found at->" + self.path.name)
          flag = true;
          initiator ! updateFingerTable(pos, self)
          // count=count+1
        }
      }

      if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name)) && k > Integer.parseInt(self.path.name) && k > Integer.parseInt(pred.path.name)) {
        //println("Successor found at->" + self.path.name)
        flag = true;
        initiator ! updateFingerTable(pos, self)
      }

      var minIndex = 0
      var min = k - (Integer.parseInt(self.path.name))
      if (!flag) {
        for ((key, value) <- fingerTable) {
          var x = k - (Integer.parseInt(value.path.name)).toInt
          if (x < 0)
            x += (math.pow(2, m).toInt)
          if (x < min) {
            min = x
            minIndex = key
          }
        }

        if (minIndex != 0) {
          flag = true
          //  count+=1
          fingerTable(minIndex) ! findfingerTableSuccessor(k, pos, initiator)
        }
      }

      if (!flag) {
        ///println((k) + "not found in current node" + self.path.name + "sending query to " + succ)
        // count+=1
        succ ! findfingerTableSuccessor(k, pos, initiator)

      }

    }







  }





}

object node{
  case class startjob(numOfRequests: Int)
  case class findfingerTableSuccessor(entry: Int, pos: Int, initiator: ActorRef)
  case class generateRequests(numOfRequests: Int)
  case object fingerTableInit
  case class notifySuccessorPredecessor(succ: ActorRef, pred: ActorRef)
  case object printNeighbours
  case class nodeJoin(existingNode: ActorRef)
  case class setNeighbours(p: ActorRef, s: ActorRef)
  case class updateFingerTable(pos: Int, self: ActorRef)
  case class findFileWithHop(initiator: Int, k: Int, count: Int)
  case class updateOthers(initiator: ActorRef, pred: ActorRef, succ: ActorRef)
  case class increment(count: Int)
  case class findSuccessor(k: Int, initiator: ActorRef)
  case class fixSuccessor(nodes: ActorRef)
  case class fixPredecessor(nodes: ActorRef)

  case class setFingerTable(map: SortedMap[Int, ActorRef])


}
