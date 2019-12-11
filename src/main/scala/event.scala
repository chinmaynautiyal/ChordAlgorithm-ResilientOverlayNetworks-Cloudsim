package com.nautiyalraj


sealed trait event

/**
 *
 * ;unused
 */
object event {

  final case class FingerReset(nodeId: Long, index: Int) extends event

  final case class FingerUpdated(nodeId: Long, index: Int, fingerId: Long) extends event

  final case class nodeCreated(nodeId: Long, successorId: Long) extends event

  final case class NodeShuttingDown(nodeId: Long) extends event

  final case class PredecessorReset(nodeId: Long) extends event

  final case class PredecessorUpdated(nodeId: Long, predecessorId: Long) extends event

  final case class SuccessorListUpdated(nodeId: Long, primarySuccessorId: Long, backupSuccessorIds: List[Long])
    extends event

}