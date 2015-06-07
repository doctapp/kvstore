package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))

  case class CheckPersist(snapshot: Replicator.Snapshot)
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  arbiter ! Join

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var prevSeq = 0L

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {

    case Replicas(replicas) => handleReplicas(replicas)

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Insert(key, value, id) =>
      kv += key -> value
      // TODO: Hack
      sender ! OperationAck(id)
      replicators.foreach { _ ! Replicate(key, Some(value), id) }
      // TODO: Wait for Replicated
      //OperationAck(id) | OperationFailed(id)
      //StartTick(1 second) : OperationFailed(id) -> t > 1 sec
      //persistence ! Persist(key, valueOption, id)

    case Remove(key, id) =>
      kv -= key
      // TODO: Hack
      sender ! OperationAck(id)
      replicators.foreach { _ ! Replicate(key, None, id) }
      // TODO: Wait for Replicated
      //OperationAck(id) | OperationFailed(id)
      //StartTick(1 second) : OperationFailed(id) -> t > 1 sec
      //persistence ! Persist(key, valueOption, id)

    case Persisted(key, id) =>

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {

    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    case snapshot @ Snapshot(key, valueOption, seq) =>
      val expectedSeq = prevSeq
      if (seq > expectedSeq) {
        // Ignore
      } else if (seq < expectedSeq) {
        // Ignore + Ack
        sender ! SnapshotAck(key, seq)
      } else {
        // TODO Use persistence ! Persist(key, valueOption, id)
        // TODO Once persisted, sender ! SnapshotAck(key, seq)
        context.system.scheduler.scheduleOnce(200 millis, self, CheckPersist(snapshot))
        sender ! SnapshotAck(key, seq)

        valueOption match {
          case Some(value) => kv += key -> value
          case None => kv -= key
        }

        prevSeq = seq + 1
      }

    case CheckPersist(snapshot) =>

    case Persisted(key, id) =>
  }

  private[this] def handleReplicas(replicas: Set[ActorRef]): Unit = {

    // Allocate one Replicator for each replica that joined except for self
    replicas.filter(r => r != self && replicators(r)).foreach { replica =>
      val replicator = context.actorOf(Props(new Replicator(self)))
      replicators += replicator
      secondaries += replica -> replicator

      // Send all updates
      kv.foreach { case (k, v) => replicator ! Replicate(k, Some(v), -1) }
    }

    // Stop the replicator for each replica that left
    secondaries.filter { case (replica, _) => !replicas(replica) }.foreach { case (replica, replicator) =>
      context.stop(replicator)
      secondaries -= replica
      replicators -= replicator
      // TODO Handle operations waiting on this replica
    }
  }
}
