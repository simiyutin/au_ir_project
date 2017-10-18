package ru.spbau.mit.ir.crawler

fun main(args: Array<String>) {
    val crawler = Crawler("https://stackoverflow.com/")
    crawler.crawl()
}
