package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkMessage

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class CongestionControl {

  // ack should be received within 3s,
  // otherwise, network congestion occurs.
  final val ackTimeLimit = 3000

  // if the ack for a message is not received after 60s,
  // we trigger the resending logic.
  // Note that the resend is not guaranteed to happen
  // after sending the message for 60s
  final val resendTimeLimit = 60000 // 60s

  // slow start threshold
  // after windowSize > ssThreshold,
  // we increment windowSize by one every time,
  // otherwise, we multiply windowSize by 2.
  private var ssThreshold = 16

  // initial window size = 1
  // it represents how many messages can be sent concurrently
  private var windowSize = 1

  private val toBeSent = new mutable.Queue[NetworkMessage]
  private val messageBuffer = new ArrayBuffer[NetworkMessage]()
  private val inTransit = new mutable.LongMap[NetworkMessage]()
  private val sentTime = new mutable.LongMap[Long]()

  // Note that toBeSent buffer is always empty if inTransit.size < windowSize
  def canSend: Boolean = inTransit.size < windowSize

  def enqueueMessage(data: NetworkMessage): Unit = {
    toBeSent.enqueue(data)
  }

  def ack(id: Long): Unit = {
    if (!inTransit.contains(id)) return
    inTransit.remove(id)
    if (System.currentTimeMillis() - sentTime(id) < ackTimeLimit) {
      if (windowSize < ssThreshold) {
        windowSize = Math.min(windowSize * 2, ssThreshold)
      } else {
        windowSize += 1
      }
    } else {
      ssThreshold /= 2
      if (ssThreshold < 1) {
        ssThreshold = 1
      }
      windowSize = ssThreshold
    }
    sentTime.remove(id)
  }

  def getBufferedMessagesToSend: Array[NetworkMessage] = {
    messageBuffer.clear()
    while (inTransit.size < windowSize && toBeSent.nonEmpty) {
      val msg = toBeSent.dequeue()
      inTransit(msg.messageID) = msg
      messageBuffer.append(msg)
    }
    messageBuffer.toArray
  }

  def markMessageInTransit(data: NetworkMessage): Unit = {
    inTransit(data.messageID) = data
    sentTime(data.messageID) = System.currentTimeMillis()
  }

  def getTimedOutInTransitMessages: Iterable[NetworkMessage] = {
    val timeCap = System.currentTimeMillis() - resendTimeLimit
    sentTime.collect {
      case (id, timeStamp) if timeStamp < timeCap =>
        inTransit(id)
    }
  }

  def getInTransitMessages: Iterable[NetworkMessage] = {
    inTransit.values
  }

  // join-skew research related.
  def getUnsentAndTransitMsgCount(): Long = {
    toBeSent.size + inTransit.size
  }

}
