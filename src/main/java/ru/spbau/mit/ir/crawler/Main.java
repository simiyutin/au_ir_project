package ru.spbau.mit.ir.crawler;

public class Main {

    public static void main(String[] args) {
        Crawler crawler = new Crawler("https://stackoverflow.com/");
        crawler.crawl();
    }
}
