package com.teatez.tm

import io.ktor.util.generateNonce
import kotlinx.coroutines.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.atan
import kotlin.math.tan


class FooHandler: Handler {
    data class FooTask(override val type: String, val foo: String): Task()
    override fun type(): String = "foo"
    override suspend fun handle(t: Task): Task = when(t) {
        is FooTask -> {
            println("before: ${System.currentTimeMillis()}")
            delay(1000L)
            println("after: ${System.currentTimeMillis()}")
            howMany.incrementAndGet()
            FooTask(t.type, t.foo)
        }
        else -> {println("oops"); throw Exception()}
    }
    companion object {
        val howMany = AtomicInteger(0)
    }
}

class BarHandler: Handler {
    data class BarTask(override val type: String, val bar: String): Task()
    override fun type(): String = "bar"
    override suspend fun handle(t: Task): Task = when(t) {
        is BarTask -> {
            tan(atan(tan(atan(tan(atan(867907842873.34256))))))
            howMany.incrementAndGet()
            BarTask(t.type, t.bar.reversed())
        }
        else -> {println("oops"); throw Exception()}
    }
    companion object {
        val howMany: AtomicInteger = AtomicInteger(0)
    }
}

fun main(args: Array<String>): Unit {
    val start = System.currentTimeMillis()
    println("test starting at $start")
    val tm = TaskMaker(maxWorkers = 1000, clientPort = 9896, serverPort = 9897)

    val fh = FooHandler()
    val bh = BarHandler() //hehe

    tm.register(fh)
    tm.register(bh)

    runBlocking {
        println("test started")
        val j1 = GlobalScope.async { repeat(1000) {
            tm.make(FooHandler.FooTask("foo", generateNonce())).join()
        } }

        val j2 = GlobalScope.async { repeat(1000) {
            tm.make(BarHandler.BarTask("bar", generateNonce())).join()
        } }
        j1.await()
        j2.await()
        tm.awaitClosing()
    }
    val end = System.currentTimeMillis()
    println("foos: ${FooHandler.howMany.get()}")
    println("bars: ${BarHandler.howMany.get()}")
    println("test ended in ${end - start}")
    println("start: $start \nend: $end")
}

