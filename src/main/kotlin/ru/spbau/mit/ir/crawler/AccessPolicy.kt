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
    private val timeoutMillis: Long = 20000

    private val invalidRobots = HashSet<String>()

    fun getAccess(link: URL): Access {
        if (!timeout(link.host)) return Access.DELAYED

        if (link.host.endsWith("stackexchange.com") && alowedStackexchangeSubdomens.none { link.host.startsWith(it) }) {
            return Access.DENIED
        }

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

    private val alowedStackexchangeSubdomens = listOf(
            "cstheory",
            "cs",
            "ai",
//            "meta",
            "webapps",
            "webmasters",
            "gamedev",
            "tex",
            "unix",
            "softwareengineering",
            "android",
            "security",
            "ux",
            "dba",
//            "codegolf",
            "reverseengineering",
//            "tridion",
//            "magento",
            "expressionengine",
            "robotics",
//            "salesforce",
//            "windowsphone",
            "bitcoin",
            "crypto",
            "sharepoint",
            "sqa",
//            "drupal",
            "raspberrypi",
            "networkengineering",
            "emacs",
//            "augur",
            "devops",
            "sitecore",
//            "monero",
            "retrocomputing",
            "craftcms",
//            "joomla",
            "arduino",
            "softwarerecs",
            "tor",
            "computergraphics",
            "elementaryos",
            "vi"
    )
}