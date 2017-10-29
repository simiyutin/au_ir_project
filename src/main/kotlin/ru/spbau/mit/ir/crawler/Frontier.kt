package ru.spbau.mit.ir.crawler

import java.net.MalformedURLException
import java.net.URI
import java.net.URL
import java.util.ArrayDeque
import java.util.HashSet
import java.util.Queue

class Frontier {

    private val allowedHashes = HashSet<Int>()

    private val queue: Queue<String> = ArrayDeque()
    private val visited: MutableSet<String> = HashSet()

    val done get() = queue.isEmpty()
    val size get() = queue.size

    private val maxTotalProcessed = 50000

    fun nextUrl(): URL = try {
        URL(queue.poll())
    }
    catch (e: MalformedURLException) {
        throw IllegalStateException("Malformed url in frontier.", e)
    }

    private fun canHandleUrl(url: URL) = url.host.hashCode() in allowedHashes

    fun addUrlIfCanHandle(url: URL): Boolean {
        if (queue.size + visited.size > maxTotalProcessed) return false
        if (!canHandleUrl(url)) return false
        if (visited.add(url.toExternalForm())) {
            queue.add(url.toExternalForm())
        }
        return true
    }

    fun addUrlWithNewHash(urls: List<URL>) {
        allowedHashes.addAll(urls.map { it.host.hashCode() })
        urls.forEach { addUrlIfCanHandle(it) }
    }
}
