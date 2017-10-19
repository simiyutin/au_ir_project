package ru.spbau.mit.ir.crawler

import java.io.IOException
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import java.net.URL


fun main(args: Array<String>) {
//    val crawler = Crawler("https://stackoverflow.com/")
//    val crawler = Crawler("https://en.wikipedia.org/")

    val startUrl = URL("https://en.wikipedia.org/wiki/Prime_number_theorem")

    val system = ActorSystem.create("crawler")

    try {
        val manager = system.actorOf(Props.create(Manager::class.java), "managerActor")
        system.actorOf(Props.create(Crawler::class.java, manager), "crawlerActor")

        Thread.sleep(1000) // wait for crawler to init
        manager.tell(ManagerAssignUrlHash(startUrl), ActorRef.noSender())

        System.`in`.read()
    } catch (ioe: IOException) {
    } finally {
        system.terminate()
    }
}
