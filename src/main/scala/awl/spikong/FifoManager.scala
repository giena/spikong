package awl.spikong

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import akka.pattern.pipe

object FifoProtocol {
  case object Initialize
  case class PutMessage(content:Array[Byte])
  case object PollMessage
  case object PeekMessage
  case object DeleteMessage
}

object FifoManager {
  def props(fifoName:String, config:Config): Props = Props(new FifoManager(fifoName,config))
}

/**
  * Created by giena on 04/03/17.
  */
class FifoManager (fifoName:String, config:Config) extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global
  import FifoProtocol._
  val fifo = new AeroFifo(fifoName,config)

  def receive = starting

  def starting(): Receive = {
    case Initialize =>
      fifo.initialize()
      context.become(managing)
    case _ => throw new IllegalStateException(s"Fifo ${fifoName} is not yet initialized")
  }

  def managing(): Receive = {
    case Initialize =>
    case PutMessage(content) => fifo.addMessage(content)
    case PollMessage =>
      val s = sender()
      pipe(fifo.pollMessage)  to s
    case PeekMessage =>
      val s = sender()
      pipe(fifo.peekMessage) to s
    case DeleteMessage =>
      val s = sender()
      pipe(fifo.removeMessage) to s
  }
}
