package ru.spbau.mit.ir.crawler

import akka.actor.AbstractActor
import akka.actor.ActorRef
import akka.pattern.Patterns
import akka.util.Timeout
import java.net.URL
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import java.util.concurrent.TimeoutException

sealed class ManagerRequest

data class ManagerAddCrawler(val crawler: ActorRef) : ManagerRequest()
data class ManagerAssignUrlHash(val url: URL, val busyness: Int) : ManagerRequest()
object ManagerPrintTotalCrawled : ManagerRequest()

class Manager : AbstractActor() {

    private val crawlerRequestTimeout = Timeout(2000, TimeUnit.SECONDS)

    private val crawlersBusyness = HashMap<ActorRef, Pair<Int, Long>>()
    private val busynessRefreshmentDelay = 20000

    private val responsibleForHash = HashMap<Int, ActorRef>()

    override fun createReceive() = receiveBuilder().match(ManagerAddCrawler::class.java) { msg ->
        crawlersBusyness[msg.crawler] = Pair(0, System.currentTimeMillis())
        msg.crawler.tell(CrawlerCrawl, self)
    }.match(ManagerAssignUrlHash::class.java) { msg ->
        val hostHash = msg.url.host.hashCode()
        if (crawlersBusyness.containsKey(sender)) {
            crawlersBusyness[sender] = Pair(msg.busyness, System.currentTimeMillis())
        }
        responsibleForHash[hostHash]?.tell(CrawlerAddUrl(msg.url), self) ?: assignNewHash(msg.url)
    }.match(ManagerPrintTotalCrawled.javaClass) {
        println("Total pages crawled: ${getTotalCrawled()}      !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    }.build()!!

    private fun assignNewHash(url: URL) {
        if (crawlersBusyness.isEmpty()) throw IllegalStateException("There are no crawlers ):")
        refreshBusyness()

        val (laziestCrawler, laziestBusyness) = crawlersBusyness.minBy { it.value.first }!!
        responsibleForHash[url.host.hashCode()] = laziestCrawler
        laziestCrawler.tell(CrawlerAddUrl(url), self)

        crawlersBusyness[laziestCrawler] = Pair(laziestBusyness.first, laziestBusyness.second + 10) // empirical thing
    }

    private fun refreshBusyness() {
        val futures = crawlersBusyness.keys.mapNotNull { crawler ->
            if (System.currentTimeMillis() - crawlersBusyness[crawler]!!.second > busynessRefreshmentDelay) {
                Pair(crawler, Patterns.ask(crawler, CrawlerRequestFrontierSize, crawlerRequestTimeout))
            } else null
        }
        futures.forEach { (crawler, future) ->
            try {
                val result = Await.result(future, crawlerRequestTimeout.duration()) as CrawlerReportFrontierSize
                crawlersBusyness[crawler] = Pair(result.size, System.currentTimeMillis())
            } catch (e : TimeoutException) {
                // do nothing (maybe increase busyness ?...)
            }
        }
    }

    private fun getTotalCrawled(): Int {
        if (crawlersBusyness.isEmpty()) return 0
        val futures = crawlersBusyness.keys.map { crawler ->
            Pair(crawler, Patterns.ask(crawler, CrawlerRequestProcessed, crawlerRequestTimeout))
        }
        return futures.map { (crawler, future) ->
            try {
                (Await.result(future, crawlerRequestTimeout.duration()) as CrawlerReportProcessed).number
            } catch (e : TimeoutException) {
                0
            }
        }.sum()
    }
}