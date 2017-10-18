package ru.spbau.mit.ir.crawler;

import ru.spbau.mit.ir.crawler.crawler2.Crawler2;

public class Main {

    public static void main(String[] args) {
        Crawler crawler = new Crawler2("https://en.wikipedia.org");
        crawler.crawl();
    }
}
