package ru.spbau.mit.ir.crawler

import java.io.IOException
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props


fun main(args: Array<String>) {
//    val crawler = Crawler("https://stackoverflow.com/")
//    val crawler = Crawler("https://en.wikipedia.org/")

    val system = ActorSystem.create("crawler")

    try {
        val crawler = system.actorOf(Props.create(Crawler::class.java, "https://en.wikipedia.org/"), "crawlerActor")
        crawler.tell(CrawlerCrawl, ActorRef.noSender())

        System.`in`.read()
    } catch (ioe: IOException) {
    } finally {
        system.terminate()
    }
}
