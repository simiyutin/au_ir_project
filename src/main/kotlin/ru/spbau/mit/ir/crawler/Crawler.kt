package ru.spbau.mit.ir.crawler

import org.jsoup.Jsoup

import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.HttpURLConnection
import java.net.MalformedURLException
import java.net.URL
import java.util.*
import com.panforge.robotstxt.RobotsTxt
import org.apache.commons.lang3.mutable.Mutable
import sun.net.www.protocol.http.HttpURLConnection.userAgent




class Crawler(initialUrl: String) {

    private val frontier = Frontier().apply { this.addUrl(initialUrl) }
    private val visitedPages: MutableSet<Int> = HashSet()
    private val userAgent = "spbauCrawler"
    private val lastVisitTimes: MutableMap<String, Long> = HashMap()
    private val timeoutMillis: Long = 2000

    private val robotsTxtMap: MutableMap<String, RobotsTxt> = HashMap() // todo: вынести

    private var processed: Int = 0

    fun crawl() {
        while (!frontier.done) {
            val website = frontier.nextSite()
            val url = website.nextUrl()
            val hash = url.hashCode()

            if (!visitedPages.add(hash)) continue

            val link: URL
            try {
                link = URL(url)
            }
            catch (e : MalformedURLException) {
                e.printStackTrace()
                continue
            }

            if (!timeout(link.host)) {
                frontier.addUrl(url)
                continue
            }

            if (!robotsTxtMap.containsKey(link.host)) {
                val robotsTxt = retrieveRobotsTxt(link) ?: continue
                robotsTxtMap.put(link.host, robotsTxt)
            }

            val robotsTxt = robotsTxtMap[link.host]!!
            if (!permits(robotsTxt, link)) continue

            val html = retrieveUrl(link)
            if (html != null) {
                storeDocument(url, html)
                val nestedUrls = parseUrls(html)
                nestedUrls.forEach { frontier.addUrl(it) }
            }
            frontier.releaseSite(website)
            processed++

            lastVisitTimes.put(link.host, System.currentTimeMillis())

            println("queue size:${frontier.size}, processed:$processed")
        }
    }

    private fun permits(robotsTxt: RobotsTxt, link: URL): Boolean {
        return robotsTxt.query(userAgent, link.toExternalForm())
    }

    private fun retrieveRobotsTxt(link : URL): RobotsTxt? {
        try {
            val path = "${link.protocol}://${link.host}/robots.txt"
            URL(path).openStream().use {
                return RobotsTxt.read(it)
            }
        } catch (e: Exception) {
            e.printStackTrace()
            return null
        }
    }


    private fun timeout(host: String): Boolean {
        if (!lastVisitTimes.containsKey(host)) {
            return true
        }
        val lastVisitTime = lastVisitTimes[host]!!
        return System.currentTimeMillis() - lastVisitTime >= timeoutMillis
    }

    private fun retrieveUrl(link: URL): String? {
        var connection: HttpURLConnection? = null
        try {
            connection = link.openConnection() as HttpURLConnection
            connection.addRequestProperty("User-Agent", userAgent)

            val HttpConnectionRedirectStatus = listOf(
                    HttpURLConnection.HTTP_MOVED_TEMP,
                    HttpURLConnection.HTTP_MOVED_PERM,
                    HttpURLConnection.HTTP_SEE_OTHER
            )

            val status = connection.responseCode

            val redirect = status in HttpConnectionRedirectStatus
            if (status != HttpURLConnection.HTTP_OK) return null

            if (redirect) {
                val newUrl = connection.getHeaderField("Location")
                return try {
                    retrieveUrl(URL(newUrl))
                }
                catch (e : MalformedURLException) {
                    e.printStackTrace()
                    null
                }
            }

            return InputStreamReader(connection.inputStream).buffered().use {
                it.lineSequence().joinToString("\n")
            }
        } catch (e: Exception) {
            e.printStackTrace()
            return null
        } finally {
            if (connection != null) {
                connection.disconnect()
            }
        }
    }

    private fun storeDocument(url: String, text: String) {
        val doc = Jsoup.parse(text)
        val body = doc.body()
        val path = "crawled/"
        try {
            PrintWriter(path + url.replace('/', '_') + ".txt", "UTF-8").use { writer -> writer.print(body) }
        } catch (ex: Exception) {
            ex.printStackTrace()
        }

    }

    private fun parseUrls(text: String): List<String> {
        val doc = Jsoup.parse(text)
        val body = doc.body()
        val links = body.select("a")
        return links.eachAttr("abs:href")
    }
}
