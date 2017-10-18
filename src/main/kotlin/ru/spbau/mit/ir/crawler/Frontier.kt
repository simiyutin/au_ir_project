package ru.spbau.mit.ir.crawler

import java.util.ArrayDeque
import java.util.Queue

class Frontier {

    private val queue: Queue<String> = ArrayDeque<String>()

    val done get() = queue.isEmpty()
    val size get() = queue.size

    fun nextSite() = Website(queue.poll())
    fun addUrl(url: String) = queue.add(url)

    fun releaseSite(website: Website) {
        // do nothing
    }
}
