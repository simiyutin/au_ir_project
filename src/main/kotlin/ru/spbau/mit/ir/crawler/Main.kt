package ru.spbau.mit.ir.crawler

fun main(args: Array<String>) {
    val crawler = Crawler("https://en.wikipedia.org/wiki/Prime_number")
    crawler.crawl()
}
