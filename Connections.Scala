class Server extends Actor {

    import Tcp._
    import context.system

    IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 0))

    def receive = {
        case b @ Bound(localAddress) => 
        context.parent ! b
        
        case CommandFailed(_: Bind) => context.stop(self)

        case c @ Connected(remote, local) =>
        val handler    = context.actorOf(Props[SimplisticHandler])
        val connection = sender()
        connection ! Register(handler)
    }
}