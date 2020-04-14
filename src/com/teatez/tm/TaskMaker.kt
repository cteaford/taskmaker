package com.teatez.tm

import com.daveanthonythomas.moshipack.MoshiPack
import kotlinx.atomicfu.AtomicInt
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import java.io.DataOutputStream
import java.io.Serializable
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

abstract class Task: Serializable {
    abstract val type: String
    class CloseTask(val closer: () -> Unit): Task() {
        override val type: String = "close"
    }
}

interface Handler {
    fun type(): String
    suspend fun handle(t: Task): Task
}

typealias Handlers = Map<String, Handler>
typealias Worker = SendChannel<Task>
typealias Workers = List<Worker>
class TaskMaker (
        private val maxWorkers: Int = 5,
        private val serverMode: Boolean = false,
        private val addr: String = "localhost",
        private val clientPort: Int = 9896,
        private val serverPort: Int = 9897,
        private val forward: Boolean = serverMode) : CoroutineScope by CoroutineScope(Dispatchers.Default) {

    private val client: Client by lazy { client(addr) }
    private val server: Server by lazy { server(muxer) }
    private val state = AtomicBoolean(false) //up or down
    private val muxer: SendChannel<Message<*>> by lazy { TaskMuxer(maxWorkers, {state.set(true)},{state.set(false)}).make() }
    fun make(t: Task) = launch { muxer.send(HandleTaskMsg(t)) }
    fun register(h: Handler) = launch { muxer.send(AddHandlerMsg(h)) }
    suspend fun close() =  muxer.send(CloseMessage()).also { muxer.close() }
    suspend fun awaitClosing() = muxer.send(CloseMessage()).also { while(state.get()){} }.also { muxer.close() }

    /**
     * A class representing multiple user created handlers as 1 handler.
     */
    private data class CompoundHandler(val f: Handler, val g: Handler): Handler {
        override fun type(): String = f.type()
        override suspend fun handle(t: Task): Task = f.handle(g.handle(t))
    }

    /**
     * Messages are used to coordinate tasks between 'user threads'
     * and the worker threads. All messaging is async so methods
     * sending messages need to release immediately(not block the thread).
     */
    sealed class Message<T> {
        abstract val value: T
        abstract class HandlerMessage<T>: Message<T>()
        abstract class TaskMessage<T>: Message<T>()
        abstract class SystemMessage<T>: Message<T>()
    }
    data class AddHandlerMsg(override val value: Handler): Message.HandlerMessage<Handler>()
    data class HandleTaskMsg(override val value: Task): Message.TaskMessage<Task>()
    class CloseMessage: Message.SystemMessage<Unit>() {
        override val value: Unit = Unit
    }

    private fun server(sc: SendChannel<Message<*>>) = Server(sc, serverPort)
    private class Server(val sc: SendChannel<Message<*>>, port: Int) : CoroutineScope by CoroutineScope(Dispatchers.Default) {
        private val ss = ServerSocket(port)
        val serving = atomic(true)
        private suspend fun serve() = withContext(Dispatchers.IO) {
            while(serving.value) {
                val s = ss.accept()
                SocketConnection(s, sc) //where should i put these????
            }
        }
        suspend fun stop() = withContext(Dispatchers.IO) {
            serving.lazySet(false)
            ss.close()
        }
        fun start() = launch { serve() }
    }

    private class SocketConnection(val s: Socket, sc: SendChannel<Message<*>>) : CoroutineScope by CoroutineScope(Dispatchers.Default){
        private val reader: Scanner = Scanner(s.getInputStream())
        private suspend fun close() = withContext(Dispatchers.IO){
            s.close()
        }

        init { launch {
            var cont = true
            while(cont) {
                reader.nextLine().let { line ->
                    when(val msg = deserialize(line)) {
                        is CloseMessage -> {
                            close()
                            cont = false
                        }
                        else -> sc.send(msg)
                    }
                }
            }
        }
        }
    }

    private fun client(addr: String) = Client(addr, clientPort)
    private class Client(addr: String, port: Int) {
        private val s by lazy { Socket(addr, port) }
        private val dos by lazy { DataOutputStream(s.getOutputStream()) }
        fun send(m: Message<*>) = dos.write(serialize(m))
        fun close() { s.close(); dos.close()}
    }

    /**
     * the TaskMuxer(Multiplexer) is the core of task maker.
     * It coordinates inbound messages on its own coroutine.
     * the muxer is just an actor like the other workers but
     * it hands out tasks to the appropriate worker.
     */
    private class TaskMuxer(val maxWorkers: Int, val onOpen: () -> Unit, val onClose: () -> Unit): CoroutineScope by CoroutineScope(Dispatchers.Default){
        /**
         * Workers are kotlin actors( a coroutine linked to a recieve channel)
         * A worker holds a handler and passes messages to it (potentially blocking)
         */
        private fun makeWorker(handler: Handler) = actor<Task>(
            capacity = 1000,
            start = CoroutineStart.LAZY) {
            for (task in channel) {
                if (task is Task.CloseTask) {
                    channel.close()
                    task.closer()
                } else handler.handle(task)
            }
        }

        fun make() = actor<Message<*>> {
            onOpen()
            val workerQueues: MutableMap<String, Workers> = mutableMapOf()
            fun closeWorkers(q: String) {
                val workers = AtomicInteger(workerQueues[q]?.size ?: 0)
                workerQueues[q]?.map { w ->
                    launch { w.send(Task.CloseTask(closer = {workers.decrementAndGet()})) }
                }
                while (workers.get() > 0) { /*this loop will spin out until the workers close */}
                onClose()
            }

            fun closeQueues() {
                println("closing queues")
                for(q in workerQueues.keys) {
                    closeWorkers(q)
                }
                println("queues closed")
            }
            var handlers: Handlers = mapOf()
            for (msg in channel) {
                when(msg) {
                    is HandleTaskMsg -> {
                        val type = msg.value.type
                        val task = msg.value
                        val handler = handlers[type]
                        handler?.let { //silently ignore tasks without handlers for now
                            val (w, ws) = fetchWorker(workerQueues[type] ?: LinkedList(), handler, maxWorkers, ::makeWorker)
                            workerQueues[type] = ws
                            w.send(task)
                        }
                    }
                    is AddHandlerMsg -> {
                        handlers = handlers + msg.value
                    }
                    is CloseMessage -> {
                        closeQueues()
                    }
                }
            }
        }
    }

    companion object {

        private fun forward(m: Message<*>, c: Client) = c.send(m)

        private fun fetchWorker(ws: Workers, h: Handler, max: Int, makeWorker: (Handler) -> Worker): Pair<Worker, Workers> =
            if (shouldMake(ws, max)) {
                val w = makeWorker(h)
                val next = ws + w
                Pair(w, next)
            } else {
                val w = ws[0]
                val next = (ws - w) + w //this looks dumb but it moves the worker to the end like a pop push
                Pair(w, next)
            }

        private fun shouldMake(ws: Workers, max: Int): Boolean = if (ws.isEmpty()) true else ws.size < max

        private val mp: MoshiPack = MoshiPack()
        private fun serialize(m: Message<*>): ByteArray{
            val ser = mp.pack(m)
            return Base64.getEncoder().encode(ser.readByteArray())
        }

        private fun deserialize(m: String): Message<*> {
            val decoded = Base64.getDecoder().decode(m)
            return mp.unpack(decoded)
        }

        operator fun Handlers.plus(h: Handler): Handlers {
            val n = h.type()
            val _h = this[n]
            return if (_h == null) this + mapOf(n to h)
            else this + mapOf(n to CompoundHandler(h, _h))
        }

    }
}
