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

    fun nextUrl(): URL = try {
        URL(queue.poll())
    }
    catch (e: MalformedURLException) {
        throw IllegalStateException("Malformed url in frontier.", e)
    }

    fun canHandleUrl(url: URL) = url.host.hashCode() in allowedHashes

    fun addUrl(url: URL): Boolean {
        assert(canHandleUrl(url))
        if (visited.add(url.toExternalForm())) {
            queue.add(url.toExternalForm())
        }
        return true
    }

    fun addUrlWithNewHash(url: URL) {
        allowedHashes.add(url.host.hashCode())
        addUrl(url)
    }
}