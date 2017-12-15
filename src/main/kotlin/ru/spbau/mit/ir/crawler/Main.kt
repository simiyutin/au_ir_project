package ru.spbau.mit.ir.crawler

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import java.io.IOException
import java.net.URL


fun main(args: Array<String>) {
//    val startUrl = URL("https://en.wikipedia.org/wiki/Python_(programming_language)")
//    val startUrl = URL("https://en.wikipedia.org/wiki/Java_(programming_language)")
//    val startUrl = URL("https://en.wikipedia.org/wiki/C%2B%2B")
//    val startUrl = URL("https://en.wikipedia.org/wiki/Haskell_(programming_language)")
//    val startUrl = URL("https://stackoverflow.com/questions/3437059/does-python-have-a-string-contains-substring-method")
    val startUrl = URL("https://en.wikipedia.org/wiki/Binary_Search")


    val system = ActorSystem.create("crawler")

    try {

        val manager = system.actorOf(Props.create(Manager::class.java), "managerActor")
        (1..10).forEach { system.actorOf(Props.create(Crawler::class.java, Pair(manager, it)), "crawlerActor$it") }

        Thread.sleep(5000) // wait for crawler to init
        manager.tell(ManagerAssignUrlsHash(listOf(startUrl), 0), ActorRef.noSender())

        while (true) {
            Thread.sleep(10000)
            manager.tell(ManagerPrintTotalCrawled, ActorRef.noSender())
        }

    } catch (ioe: IOException) {
        ioe.printStackTrace()
    } finally {
        system.terminate()
    }
}
