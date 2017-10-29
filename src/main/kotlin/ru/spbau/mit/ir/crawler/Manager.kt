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
data class ManagerAssignUrlsHash(val urls: List<URL>, val busyness: Int) : ManagerRequest()
object ManagerPrintTotalCrawled : ManagerRequest()

class Manager : AbstractActor() {

    private val crawlerRequestTimeout = Timeout(1000, TimeUnit.SECONDS)

    private val crawlersBusyness = HashMap<ActorRef, Pair<Int, Long>>()
    private val busynessRefreshmentDelay = 20000

    private val responsibleForHash = HashMap<Int, ActorRef>()

    override fun createReceive() = receiveBuilder().match(ManagerAddCrawler::class.java) { msg ->
        crawlersBusyness[msg.crawler] = Pair(0, System.currentTimeMillis())
        msg.crawler.tell(CrawlerCrawl, self)
    }.match(ManagerAssignUrlsHash::class.java) { msg ->
        if (crawlersBusyness.containsKey(sender)) {
            crawlersBusyness[sender] = Pair(msg.busyness, System.currentTimeMillis())
        }
        msg.urls.forEach { assignNewHash(it) }
        msg.urls.groupBy { responsibleForHash[it.host.hashCode()]!! }.forEach { (crawler, urls) ->
            crawler.tell(CrawlerAddUrls(urls), self)
        }
    }.match(ManagerPrintTotalCrawled.javaClass) {
        println("Total pages crawled: ${getTotalCrawled()}")
    }.build()!!

    private fun assignNewHash(url: URL) {
        if (crawlersBusyness.isEmpty()) throw IllegalStateException("There are no crawlers ):")

        val (laziestCrawler, _) = crawlersBusyness.minBy { it.value.first }!!
        responsibleForHash[url.host.hashCode()] = laziestCrawler

        refreshBusyness(laziestCrawler)
    }

    private fun refreshBusyness(crawler: ActorRef) {
        if (System.currentTimeMillis() - crawlersBusyness[crawler]!!.second <= busynessRefreshmentDelay) {
            slightlyIncreaseBusyness(crawler)
            return
        }
        val future = Patterns.ask(crawler, CrawlerRequestFrontierSize, crawlerRequestTimeout)
        try {
            val result = Await.result(future, crawlerRequestTimeout.duration()) as CrawlerReportFrontierSize
            crawlersBusyness[crawler] = Pair(result.size, System.currentTimeMillis())
        } catch (e : TimeoutException) {
            slightlyIncreaseBusyness(crawler)
        }
    }

    private fun slightlyIncreaseBusyness(crawler: ActorRef) {
        val oldBusyness = crawlersBusyness[crawler]!!
        crawlersBusyness[crawler] = Pair(oldBusyness.first + 10, oldBusyness.second) // empirical thing
    }

    private fun getTotalCrawled(): Int {
        if (crawlersBusyness.isEmpty()) return 0
        val futures = crawlersBusyness.keys.map { crawler ->
            Patterns.ask(crawler, CrawlerRequestProcessed, crawlerRequestTimeout)
        }
        return futures.map { future ->
            try {
                (Await.result(future, crawlerRequestTimeout.duration()) as CrawlerReportProcessed).number
            } catch (e : TimeoutException) {
                0
            }
        }.sum()
    }
}