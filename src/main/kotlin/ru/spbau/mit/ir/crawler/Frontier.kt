package ru.spbau.mit.ir.crawler

import java.net.MalformedURLException
import java.net.URL
import java.util.*

//todo: очередь с приоритетами сайтов. в каждом сайте храним время поседнего посещения и очередь его ссылок
//todo: при взятии новой ссылки, берем самый давний сайт, ждем остаток таймаута, если он есть, берем ссылку из него,
//todo: обновляем время последнего посещения, возвращаем в очередь

class Frontier {

    private val allowedHashes = HashSet<Int>()

    private val queue: Queue<String> = ArrayDeque()
    private val visited: MutableSet<String> = HashSet()

    val done get() = queue.isEmpty()
    val size get() = queue.size

    private val maxTotalProcessed = 5000

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

    private val wikiSites = listOf(
            "wikipedia.org",
            "wikibooks.org"
    )

    private val allowedWikiSubdomains = listOf(
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
        //todo отправлять манагеру сообщение удалить себя из списка
        if (queue.size + visited.size > maxTotalProcessed) return false

        if (!goodSite(url)) return false

        if (!isInScopeOfResponsibility(url)) return false

        val processedString = cutOffRequests(url.toExternalForm())
        if (visited.add(processedString)) {
            queue.add(processedString)
        }

        return true
    }

    fun alreadyVisited(url: URL): Boolean {
        return visited.contains(cutOffRequests(url.toExternalForm()))
    }

    fun addUrlWithNewHash(urls: List<URL>) {
        allowedHashes.addAll(urls.map { it.host.hashCode() })
        urls.forEach { addUrlIfCanHandle(it) }
    }

    fun goodSite(url: URL): Boolean {

        if (allowedTopLevelDomains.none { url.host.endsWith(it) }) {
            return false
        }

        if (declinedDomains.any{ url.host.contains(it) }) {
            return false
        }

        if (url.host.endsWith("stackexchange.com") && alowedStackexchangeSubdomains.none { url.host.startsWith(it) }) {
            return false
        }

        if (wikiSites.any { url.host.endsWith(it) } && allowedWikiSubdomains.none { url.host.startsWith(it) }) {
            return false
        }

        return true
    }

    private fun isInScopeOfResponsibility(url: URL) = url.host.hashCode() in allowedHashes

    fun cutOffRequests(url: String) : String {
        return url.split('#')[0].split('?')[0]
    }
}
