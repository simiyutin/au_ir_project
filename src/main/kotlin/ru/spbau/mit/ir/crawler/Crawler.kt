package ru.spbau.mit.ir.crawler

import akka.actor.AbstractActor
import akka.actor.ActorRef
import org.jsoup.Jsoup

import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.HttpURLConnection
import java.net.MalformedURLException
import java.net.URL

sealed class CrawlerRequest

object CrawlerCrawl : CrawlerRequest()
data class CrawlerAddUrl(val url: URL) : CrawlerRequest()
object CrawlerRequestFrontierSize : CrawlerRequest()
object CrawlerRequestProcessed : CrawlerRequest()

sealed class CrawlerResponse

data class CrawlerReportFrontierSize(val size: Int) : CrawlerResponse()
data class CrawlerReportProcessed(val number: Int) : CrawlerResponse()

class Crawler(pr: Pair<ActorRef, Int>) : AbstractActor() {
    private val manager = pr.first
    val crawlerId = pr.second
    init {
        manager.tell(ManagerAddCrawler(self), self)
    }

    override fun createReceive() = receiveBuilder().match(CrawlerCrawl.javaClass) {
        tryCrawlOnce() // todo: hide exceptions
        self.tell(CrawlerCrawl, self)
    }.match(CrawlerAddUrl::class.java) { msg ->
        frontier.addUrlWithNewHash(msg.url)
    }.match(CrawlerRequestFrontierSize.javaClass) {
        sender.tell(CrawlerReportFrontierSize(frontier.size), self)
    }.match(CrawlerRequestProcessed.javaClass) {
        sender.tell(CrawlerReportProcessed(processed), self)
    }.build()!!

    private val frontier = Frontier()
    private val userAgent = "spbauIRCrawler"

    private val accessPolicy = AccessPolicy(userAgent)

    private var processed: Int = 0

    private fun tryCrawlOnce(): Boolean {
        if (frontier.done) return false

        val link = frontier.nextUrl()

        val access = accessPolicy.getAccess(link)
        if (access == AccessPolicy.Access.DELAYED) {
            frontier.addUrl(link)
        }
        if (access != AccessPolicy.Access.GRANTED) return false

        crawlUrl(link)

        return true
    }

    private fun crawlUrl(link: URL) {
        val html = retrieveUrl(link) ?: return

        processed++
        if (processed % 100 == 0) {
            println("crawler: $crawlerId, queue size:${frontier.size}, processed:$processed")
        }
        storeDocument(link, html)

        parseUrls(html).forEach { url ->
            val newLink =
                    try {
                        URL(url)
                    } catch (e: MalformedURLException) {
                        return@forEach
                    }
            if (frontier.canHandleUrl(newLink)) {
                frontier.addUrl(newLink)
            } else {
                manager.tell(ManagerAssignUrlHash(newLink, frontier.size), self)
            }
        }
    }

    private fun retrieveUrl(link: URL): String? {
        var connection: HttpURLConnection? = null
        try {
            connection = link.openConnection() as HttpURLConnection
            connection.addRequestProperty("User-Agent", userAgent)

            val httpConnectionRedirectStatus = listOf(
                    HttpURLConnection.HTTP_MOVED_TEMP,
                    HttpURLConnection.HTTP_MOVED_PERM,
                    HttpURLConnection.HTTP_SEE_OTHER
            )

            val status = connection.responseCode

            val redirect = status in httpConnectionRedirectStatus
            if (status != HttpURLConnection.HTTP_OK) return null

            if (redirect) {
                val newUrl = connection.getHeaderField("Location")
                return try {
                    retrieveUrl(URL(newUrl))
                } catch (e: MalformedURLException) {
                    e.printStackTrace()
                    null
                }
            }

            return InputStreamReader(connection.inputStream).buffered().use {
                it.lineSequence().joinToString("\n")
            }
        } catch (e: Exception) {
            println("ERROR: Cannot retrieve url ${link.toExternalForm()}...    #############################################################")
            return null
        } finally {
            if (connection != null) {
                connection.disconnect()
            }
        }
    }

    private fun storeDocument(url: URL, text: String) {
//        println(url)
        val doc = Jsoup.parse(text)
        val body = doc.body()
        val path = "crawled/"
        try {
            PrintWriter(path + urlToFileName(url) + ".txt", "UTF-8").use { writer -> writer.print(body) }
        } catch (ex: Exception) {
            ex.printStackTrace()
        }
    }

    // to prevent FileTooLong exception
    private fun urlToFileName(url: URL) = "${url.toExternalForm().take(80).replace('/', '_')}_${url.toExternalForm().hashCode()}"

    private fun parseUrls(text: String, url: String): List<String> {
        val doc = Jsoup.parse(text, url)
        val body = doc?.body()
        val links = body?.select("a") ?: return emptyList()
        return links.eachAttr("abs:href")
    }
}
