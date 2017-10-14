package ru.spbau.mit.ir;

public class Main {

    public static void main(String[] args) {
        Crawler crawler = new Crawler("https://jsoup.org/");
        crawler.crawl();
    }
}
