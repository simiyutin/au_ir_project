package ru.spbau.mit.ir.crawler

import java.net.MalformedURLException
import java.net.URL
import java.util.*

class Frontier {

    private val allowedHashes = HashSet<Int>()

    private val queue: Queue<String> = ArrayDeque()
    private val visited: MutableSet<String> = HashSet()

    val done get() = queue.isEmpty()
    val size get() = queue.size

    private val maxTotalProcessed = 50000

    // region restrictions
    private val alowedStackexchangeSubdomains = listOf(
            "cstheory",
            "cs",
            "ai",
            "webapps",
            "webmasters",
            "gamedev",
            "tex",
            "unix",
            "softwareengineering",
            "android",
            "security",
            "ux",
            "dba",
            "reverseengineering",
            "expressionengine",
            "robotics",
            "bitcoin",
            "crypto",
            "sharepoint",
            "sqa",
            "raspberrypi",
            "networkengineering",
            "emacs",
            "devops",
            "sitecore",
            "retrocomputing",
            "craftcms",
            "arduino",
            "softwarerecs",
            "tor",
            "computergraphics",
            "elementaryos",
            "vi"
    )

    private val declinedDomains = listOf(
            "youtube.com"
    )

    private val allowedWikipediaSubdomains = listOf(
            "en"
    )

    private val allowedTopLevelDomains = listOf(
            "com",
            "org",
            "edu",
            "uk",
            "net"
    )
    // endregion

    fun nextUrl(): URL = try {
        URL(queue.poll())
    }
    catch (e: MalformedURLException) {
        throw IllegalStateException("Malformed url in frontier.", e)
    }

    fun addUrlIfCanHandle(url: URL): Boolean {
        if (queue.size + visited.size > maxTotalProcessed) return false

        if (!goodSite(url)) return false

        if (!isInScopeOfResponsibility(url)) return false

        if (visited.add(url.toExternalForm())) {
            queue.add(url.toExternalForm())
        }

        return true
    }

    fun addUrlWithNewHash(urls: List<URL>) {
        allowedHashes.addAll(urls.map { it.host.hashCode() })
        urls.forEach { addUrlIfCanHandle(it) }
    }

    private fun goodSite(url: URL): Boolean {

        if (allowedTopLevelDomains.none { url.host.endsWith(it) }) {
            return false
        }

        if (declinedDomains.any{ url.host.contains(it) }) {
            return false
        }

        if (url.host.endsWith("stackexchange.com") && alowedStackexchangeSubdomains.none { url.host.startsWith(it) }) {
            return false
        }

        if (url.host.endsWith("wikipedia.org") && allowedWikipediaSubdomains.none { url.host.startsWith(it) }) {
            return false
        }

        return true
    }

    private fun isInScopeOfResponsibility(url: URL) = url.host.hashCode() in allowedHashes
}
