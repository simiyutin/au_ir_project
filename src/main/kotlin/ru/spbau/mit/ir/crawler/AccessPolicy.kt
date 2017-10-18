package ru.spbau.mit.ir.crawler

import com.panforge.robotstxt.RobotsTxt
import java.net.URL
import java.util.HashMap

class AccessPolicy(val userAgent: String) {

    enum class Access {
        DENIED, DELAYED, GRANTED
    }

    private val robotsTxtMap: MutableMap<String, RobotsTxt> = HashMap()
    private val lastVisitTimes: MutableMap<String, Long> = HashMap()
    private val timeoutMillis: Long = 2000

    fun getAccess(link: URL): Access{
        if (!timeout(link.host)) return Access.DELAYED

        if (!robotsTxtMap.containsKey(link.host)) {
            val robotsTxt = retrieveRobotsTxt(link) ?: return Access.DENIED
            robotsTxtMap.put(link.host, robotsTxt)
        }

        val robotsTxt = robotsTxtMap[link.host]!!

        return if (robotsTxt.query(userAgent, link.toExternalForm())) {
            lastVisitTimes.put(link.host, System.currentTimeMillis())
            Access.GRANTED
        }
        else Access.DENIED
    }

    private fun retrieveRobotsTxt(link : URL): RobotsTxt? {
        try {
            val path = "${link.protocol}://${link.host}/robots.txt"
            URL(path).openStream().use {
                return RobotsTxt.read(it)
            }
        } catch (e: Exception) {
            e.printStackTrace() // todo: logging?
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