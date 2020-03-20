import akka.actor.{ Actor, ActorRef, Props }
import akka.io.{ IO, Tcp }
import akka.util.ByteString
import java.net.InetSocketAddress

//import context.system   //implicitly used by IO(Tcp)

val manager = IO(Tcp)

// The manager is an actor that handles the underlying low level I/O resources (selectors, channels) 
// and instantiates workers for specific tasks, such as listening to incoming connections.

object Client {
    def props(remote: InetSocketAddress, replies: ActorRef) = 
    Props(classOf[Client], remote, replies)
}

class Client(remote: InetSocketAddress, listener: ActorRef) extends Actor {

    import Tcp._
    import context.system

    manager ! Connect(remote)

    def receive = {
        case CommandFailed(_: Connect) =>
        listener ! "connect failed"
        context.stop(self)

        case c @ Connected(remote, local) =>
        listener ! c
        val connection = sender()   //to activate the new connection a Register message must be sent to the connection actor
        connection ! Register(self)
        context.become {

            case data: ByteString =>
            connection ! Wrire(data) 

            case CommandFailed(w: Write) =>
            // OS buffer is full
            listener ! "write failed"

            case Received(data) =>
            listener ! data

            case "close" =>
            connection ! close

//ConnectionClosed is a trait, which marks the different connection close events. 
//The last line handles all connection close events in the same way.
            case _: ConnectionClosed =>
            listener ! "connection closed"
            context.stop(self)
        }
    }
}
