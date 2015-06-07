package kvstore

import akka.actor._
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, PersistenceException}
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.{Resume, Stop, Restart}
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import scala.language.postfixOps
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

  case class AckCond(sender: ActorRef, pmsg: Persist, id: Long, toConfirm: Set[ActorRef])
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
  var expectedSeq = 0L

  val persistence = context.actorOf(persistenceProps, name = "persist")
  var persistAcks = Map.empty[String, (ActorRef, Persist, Long)]

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 10 seconds) {
    case _: PersistenceException => Resume
    case _: Exception => Stop
  }

  private[this] val tick = context.system.scheduler.schedule(100 millis, 100 millis, self, Tick)
  private[this] val oneSecondInNanos = (1 second).toNanos

  override def postStop() = tick.cancel()

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
      replicators.foreach { _ ! Replicate(key, Some(value), id) }
      store(key, Some(value), id)

    case Remove(key, id) =>
      replicators.foreach { _ ! Replicate(key, None, id) }
      store(key, None, id)

    case Persisted(key, id) =>
      persistAcks.get(key).foreach { case (sender, pmsg, t) =>
        val msg =
          if (System.nanoTime - t <= oneSecondInNanos) OperationAck(id)
          else OperationFailed(id)
        sender ! msg
        persistAcks -= key
      }

    case Tick =>
      persistAcks.values.foreach {
        case (sender, pmsg, t) =>
          if (System.nanoTime - t <= oneSecondInNanos)
            persistence ! pmsg
          else {
            sender ! OperationFailed(pmsg.id)
            persistAcks -= pmsg.key
          }
      }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    case snapshot @ Snapshot(key, valueOption, seq) =>
      if (seq > expectedSeq) {
        // Ignore
      } else if (seq < expectedSeq) {
        // Ignore + Ack
        sender ! SnapshotAck(key, seq)
      } else {
        store(key, valueOption, seq)
        expectedSeq = seq + 1
      }

    case Persisted(key, id) =>
      persistAcks.get(key).foreach { case (sender, pmsg, t) =>
        if (System.nanoTime - t <= oneSecondInNanos)
          sender ! SnapshotAck(key, id)
        persistAcks -= key
      }

    case Tick =>
      persistAcks.values.foreach {
        case (sender, pmsg, t) =>
          if (System.nanoTime - t <= oneSecondInNanos)
            persistence ! pmsg
          else
            persistAcks -= pmsg.key
      }
  }

  private def store(key: String, valueOption: Option[String], id: Long): Unit = {
    val pmsg = Persist(key, valueOption, id)
    persistence ! pmsg
    persistAcks += key -> (sender, pmsg, System.nanoTime)

    valueOption match {
      case Some(value) => kv += key -> value
      case None => kv -= key
    }
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
