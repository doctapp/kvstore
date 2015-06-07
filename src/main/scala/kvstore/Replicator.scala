package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case object Tick

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  private[this] val tick = context.system.scheduler.schedule(100 millis, 100 millis, self, Tick)

  override def postStop() = tick.cancel()

  def receive: Receive = {

    case req @ Replicate(key, valueOption, id) =>
      val seq = nextSeq
      replica ! Snapshot(key, valueOption, seq)
      acks += seq -> (sender, req)

    case SnapshotAck(key, seq) =>
      acks
        .get(seq)
        .foreach {
        case (sender, req) =>
          sender ! Replicated(key, req.id)
          acks -= seq
      }

    case Tick ⇒
      // Every 100 milliseconds, retransmit all unacknowledged changes to replica
      acks.foreach {
        case (seq, (sender, req)) => replica ! Snapshot(req.key, req.valueOption, seq)
      }
  }
}
