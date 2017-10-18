package ru.spbau.mit.ir.crawler

import org.jsoup.Jsoup

import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.HttpURLConnection
import java.net.MalformedURLException
import java.net.URL
import java.util.*


class Crawler(initialUrl: String) {

    private val frontier = Frontier().apply { this.addUrl(initialUrl) }
    private val visitedPages: MutableSet<Int> = HashSet()
    private val userAgent = "spbauCrawler"

    private val accessPolicy = AccessPolicy(userAgent)

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

            val access = accessPolicy.getAccess(link)
            if (access == AccessPolicy.Access.DELAYED) {
                frontier.addUrl(url)
            }
            if (access != AccessPolicy.Access.GRANTED) continue

            val html = retrieveUrl(link)
            if (html != null) {
                storeDocument(url, html)
                val nestedUrls = parseUrls(html)
                nestedUrls.forEach { frontier.addUrl(it) }
            }
            frontier.releaseSite(website)
            processed++

            println("queue size:${frontier.size}, processed:$processed")
        }
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
