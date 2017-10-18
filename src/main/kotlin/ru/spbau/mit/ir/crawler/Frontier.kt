package ru.spbau.mit.ir.crawler

import java.util.ArrayDeque
import java.util.HashSet
import java.util.Queue

class Frontier {

    private val queue: Queue<String> = ArrayDeque<String>()
    private val visited: MutableSet<String> = HashSet()

    val done get() = queue.isEmpty()
    val size get() = queue.size

    fun nextUrl(): String = queue.poll()

    fun addUrl(url: String) {
        if (visited.add(url)) {
            queue.add(url)
        }
    }
}
