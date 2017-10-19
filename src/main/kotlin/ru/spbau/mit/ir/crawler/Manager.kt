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
data class ManagerAssignUrlHash(val url: URL) : ManagerRequest()

class Manager : AbstractActor() {

    private val crawlerBusynessRequestTimeout = Timeout(10000, TimeUnit.SECONDS)

    private val crawlersBusyness = HashMap<ActorRef, Int>()
    private val responsibleForHash = HashMap<Int, ActorRef>()

    override fun createReceive() = receiveBuilder().match(ManagerAddCrawler::class.java) { msg ->
        crawlersBusyness[msg.crawler] = 0
        msg.crawler.tell(CrawlerCrawl, self)
    }.match(ManagerAssignUrlHash::class.java) { msg ->
        val hostHash = msg.url.host.hashCode()
        responsibleForHash[hostHash]?.tell(CrawlerAddUrl(msg.url), self) ?: assignNewHash(msg.url)
    }.build()!!

    private fun assignNewHash(url: URL) {
        if (crawlersBusyness.isEmpty()) throw IllegalStateException("There are no crawlers ):")
        val futures = crawlersBusyness.keys.map { crawler ->
            Pair(crawler, Patterns.ask(crawler, CrawlerRequestFrontierSize, crawlerBusynessRequestTimeout))
        }
        futures.forEach { (crawler, future) ->
            try {
                val result = Await.result(future, crawlerBusynessRequestTimeout.duration()) as CrawlerReportFrontierSize
                crawlersBusyness[crawler] = result.size
            } catch (e : TimeoutException) {
                // do nothing (maybe increase busyness ?...)
            }
        }
        val laziestCrawler = crawlersBusyness.minBy { it.value }!!.key
        responsibleForHash[url.host.hashCode()] = laziestCrawler
        laziestCrawler.tell(CrawlerAddUrl(url), self)
    }
}