package com.teatez.tm

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

internal class TaskMakerTest {
    class FooHandler: Handler {
        data class FooTask(override val type: String, val foo: String): Task()
        override fun type(): String = "foo"
        override suspend fun handle(t: Task): Task = when(t) {
            is FooTask -> {
                delay(1000)
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
                delay(1000)
                howMany.incrementAndGet()
                BarTask(t.type, t.bar)
            }
            else -> {println("oops"); throw Exception()}
        }
        companion object {
            val howMany: AtomicInteger = AtomicInteger(0)
        }
    }

    @Test
    fun `small test`() {
        val tm = TaskMaker(maxWorkers = 1000)

        val fh = FooHandler()
        val bh = BarHandler() //hehe

        tm.register(fh)
        tm.register(bh)

        runBlocking {
            repeat(1000) {
                tm.make(FooHandler.FooTask("foo", "foo business")).join()
                tm.make(BarHandler.BarTask("bar", "bar biz")).join()
            }
            tm.awaitClosing()
        }
        assertEquals(1000, FooHandler.howMany.get())
        assertEquals(1000, BarHandler.howMany.get())
    }
}