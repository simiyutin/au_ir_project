package ru.spbau.mit.ir.crawler

import com.panforge.robotstxt.RobotsTxt
import java.net.URL
import java.util.HashMap
import kotlin.collections.HashSet
import kotlin.collections.MutableMap
import kotlin.collections.set

class AccessPolicy(val userAgent: String) {

    enum class Access {
        DENIED, DELAYED, GRANTED
    }

    private val robotsTxtMap: MutableMap<String, RobotsTxt> = HashMap()
    private val lastVisitTimes: MutableMap<String, Long> = HashMap()
    private val timeoutMillis: Long = 5000

    private val invalidRobots = HashSet<String>()

    fun getAccess(link: URL): Access {
        if (!timeout(link.host)) return Access.DELAYED

        if (!robotsTxtMap.containsKey(link.host)) {
            val robotsTxt = retrieveRobotsTxt(link) ?: return Access.DENIED
            robotsTxtMap.put(link.host, robotsTxt)
        }

        val robotsTxt = robotsTxtMap[link.host]!!
        val robotResult = try {
            robotsTxt.query(userAgent, link.toExternalForm())
        } catch (e: Throwable) {
            println("ERROR: RobotsTxt query've just thrown ${e.javaClass}")
            return Access.DENIED // better safe that sorry (:
        }
        lastVisitTimes[link.host] = System.currentTimeMillis();
        return if (robotResult) Access.GRANTED else Access.DENIED
    }

    private fun retrieveRobotsTxt(link : URL): RobotsTxt? {
        val path = "${link.protocol}://${link.host}/robots.txt"
        if (path in invalidRobots) return null
        try {
            URL(path).openStream().use {
                return RobotsTxt.read(it)
            }
        } catch (e: Exception) {
            invalidRobots.add(path)
            println("ERROR: Cannot read $path")
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
}