package ru.spbau.mit.ir.crawler

import com.panforge.robotstxt.RobotsTxt
import java.io.IOException

import java.io.InputStream
import java.net.URL

class Website(private val rootPage: String) {

    fun nextUrl() = rootPage

    fun permitsCrawl(userAgent: String, link: URL): Boolean {
        val host = link.host
        val protocol = link.protocol
        val path = "$protocol://$host/robots.txt"

        try {
            URL(path).openStream().use { robotsTxtStream ->
                val robotsTxt = RobotsTxt.read(robotsTxtStream)
                return robotsTxt.query(userAgent, link.toExternalForm())
            }
        } catch (e: IOException) {
            e.printStackTrace() // todo: logging?
            return false
        }

    }
}
