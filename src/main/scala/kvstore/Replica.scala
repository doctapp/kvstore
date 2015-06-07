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

  case class OutstandingAck(sender: ActorRef, timestamp: Long, primaryToConfirm: Option[ActorRef], secondariesToConfirm: Set[ActorRef])
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

  private[this] var expectedSeq = 0L
  private[this] val persistence = context.actorOf(persistenceProps, name = "persist")
  private[this] var persistAcks = Map.empty[Long, (ActorRef, Persist, Long)]
  private[this] var outstandingAcks = Map.empty[Long, OutstandingAck]
  private[this] val tick = context.system.scheduler.schedule(100 millis, 100 millis, self, Tick)
  private[this] val oneSecondInNanos = (1 second).toNanos

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 10 seconds) {
    case _: PersistenceException => Resume
    case _: Exception => Stop
  }

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
      val valueOption = Some(value)
      store(key, valueOption, id)
      replicate(key, valueOption, id)
      outstandingAcks += id -> OutstandingAck(sender, System.nanoTime, Some(self), replicators)

    case Remove(key, id) =>
      store(key, None, id)
      replicate(key, None, id)
      outstandingAcks += id -> OutstandingAck(sender, System.nanoTime, Some(self), replicators)

    case Persisted(key, id) =>
      outstandingAcks.get(id).foreach { o =>
        outstandingAcks += id -> o.copy(primaryToConfirm = None)
        processOutstanding(id, System.nanoTime)
      }
      persistAcks -= id

    case Replicated(key, id) =>
      outstandingAcks.get(id).foreach { o =>
        outstandingAcks += id -> o.copy(secondariesToConfirm = o.secondariesToConfirm - sender)
        processOutstanding(id, System.nanoTime)
      }

    case Tick =>
      val now = System.nanoTime
      outstandingAcks.keys.foreach(processOutstanding(_, now))
      persistAcks.foreach {
        case (id, (sender, msg, t)) =>
          if (now - t <= oneSecondInNanos)
            persistence ! msg
          else
            persistAcks -= id
      }
  }

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
      persistAcks.get(id).foreach { case (sender, _, t) =>
        if (System.nanoTime - t <= oneSecondInNanos)
          sender ! SnapshotAck(key, id)
        persistAcks -= id
      }

    case Tick =>
      val now = System.nanoTime
      persistAcks.foreach {
        case (id, (sender, msg, t)) =>
          if (now - t <= oneSecondInNanos)
            persistence ! msg
          else
            persistAcks -= id
      }
  }

  private def processOutstanding(id: Long, now: Long): Unit = {
    outstandingAcks.get(id).foreach { o =>
      if (now - o.timestamp <= oneSecondInNanos) {
        if (o.primaryToConfirm.isEmpty && o.secondariesToConfirm.isEmpty) {
          o.sender ! OperationAck(id)
          outstandingAcks -= id
        }
      } else {
        o.sender ! OperationFailed(id)
        outstandingAcks -= id
      }
    }
  }

  private def store(key: String, valueOption: Option[String], id: Long): Unit = {
    val msg = Persist(key, valueOption, id)
    persistence ! msg
    persistAcks += id -> (sender, msg, System.nanoTime)

    valueOption match {
      case Some(value) => kv += key -> value
      case None => kv -= key
    }
  }

  private def replicate(key: String, valueOption: Option[String], id: Long): Unit = {
    val msg = Replicate(key, valueOption, id)
    replicators.foreach(_ ! msg)
  }

  private def handleReplicas(replicas: Set[ActorRef]): Unit = {
    // Allocate one Replicator for each replica that joined except for self
    replicas.filter(r => r != self && !replicators.contains(r)).foreach { replica =>
      val replicator = context.actorOf(Props(new Replicator(replica)))
      replicators += replicator
      secondaries += replica -> replicator

      // Send all updates
      kv.foreach { case (k, v) => replicator ! Replicate(k, Some(v), -1) }
    }

    // Stop the replicator for each replica that left
    secondaries.filter { case (replica, _) => !replicas.contains(replica) }.foreach { case (replica, replicator) =>
      context.stop(replicator)
      secondaries -= replica
      replicators -= replicator

      val now = System.nanoTime
      outstandingAcks.filter(_._2.secondariesToConfirm.contains(replicator)).foreach { case (id, o) =>
        outstandingAcks += id -> o.copy(secondariesToConfirm = o.secondariesToConfirm - replicator)
        processOutstanding(id, now)
      }
    }
  }
}
