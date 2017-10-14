package ru.spbau.mit.ir.crawler;

import com.panforge.robotstxt.RobotsTxt;

import java.io.InputStream;
import java.net.URL;

public class Website {
    private String rootPage;

    public Website(String rootPage) {
        this.rootPage = rootPage;
    }

    public String nextUrl() {
        return rootPage;
    }

    public boolean permitsCrawl(String userAgent, String url) {
        try {
            URL link = new URL(url);
            String host = link.getHost();
            String protocol = link.getProtocol();
            String path = protocol + "://" + host + "/robots.txt";
            try (InputStream robotsTxtStream = new URL(path).openStream()) {
                RobotsTxt robotsTxt = RobotsTxt.read(robotsTxtStream);
                boolean hasAccess = robotsTxt.query(userAgent, url);
                return hasAccess;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
