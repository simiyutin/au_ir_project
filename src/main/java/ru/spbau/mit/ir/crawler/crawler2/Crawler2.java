package ru.spbau.mit.ir.crawler.crawler2;

import com.panforge.robotstxt.RobotsTxt;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import ru.spbau.mit.ir.crawler.Crawler;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

public class Crawler2 implements Crawler {

    private final Queue<String> urls;
    private final Set<String> visitedPages;
    private final Map<String, Long> lastVisitTimes;
    private final Map<String, RobotsTxt> robotsTxtMap;
    private final int timeoutMillis = 10000;
    private final String userAgent = "spbauCrawler";

    public Crawler2(String initialUrl) {
        urls = new ArrayDeque<>();
        visitedPages = new HashSet<>();
        lastVisitTimes = new HashMap<>();
        robotsTxtMap = new HashMap<>();
        urls.add(initialUrl);
    }

    @Override
    public void crawl() {
        int processed = 0;
        while (!urls.isEmpty()) {
            String url = urls.poll();
            if (url == null) {
                continue;
            }

            if (!visitedPages.add(url)) {
                continue;
            }

            String host = Website.getHost(url);
            if (!timeout(host)) {
                urls.add(url);
                continue;
            }

            if (!robotsTxtMap.containsKey(host)) {
                RobotsTxt robotsTxt = retrieveRobotsTxt(host, Website.getProtocol(url));
                if (robotsTxt == null) {
                    continue;
                }
                robotsTxtMap.put(host, robotsTxt);
            }

            RobotsTxt robotsTxt = robotsTxtMap.get(host);
            if (!permits(robotsTxt, url)) {
                continue;
            }

            String html = retrieveUrl(url);
            if (html != null) {
                storeDocument(url, html);
                List<String> nestedUrls = parseUrls(html);
                urls.addAll(nestedUrls);
            }

            lastVisitTimes.put(host, System.currentTimeMillis());

            processed++;
            System.out.println(String.format("queue size:%s, processed:%s", urls.size(), processed));
        }
    }

    private boolean permits(RobotsTxt robotsTxt, String url) {
        return robotsTxt.query(userAgent, url);
    }

    private RobotsTxt retrieveRobotsTxt(String host, String protocol) {
        try {
            String path = protocol + "://" + host + "/robots.txt";
            try (InputStream robotsTxtStream = new URL(path).openStream()) {
                RobotsTxt robotsTxt = RobotsTxt.read(robotsTxtStream);
                return robotsTxt;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private boolean timeout(String host) {
        if (host == null) {
            return false;
        }
        if (!lastVisitTimes.containsKey(host)) {
            return true;
        }
        long lastVisitTime = lastVisitTimes.get(host);
        return System.currentTimeMillis() - lastVisitTime >= timeoutMillis;
    }

    private String retrieveUrl(String targetURL) {
        HttpURLConnection connection = null;

        try {
            URL url = new URL(targetURL);
            connection = (HttpURLConnection) url.openConnection();
            connection.addRequestProperty("User-Agent", userAgent);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                            connection.getInputStream()));

            boolean redirect = false;
            int status = connection.getResponseCode();
            if (status != HttpURLConnection.HTTP_OK) {
                if (status == HttpURLConnection.HTTP_MOVED_TEMP
                        || status == HttpURLConnection.HTTP_MOVED_PERM
                        || status == HttpURLConnection.HTTP_SEE_OTHER) {

                    redirect = true;
                } else {
                    return null;
                }
            }

            if (redirect) {
                String newUrl = connection.getHeaderField("Location");
                return retrieveUrl(newUrl);
            }

            StringBuilder sb = new StringBuilder();
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                sb.append(inputLine);
                sb.append('\n');
            }
            in.close();

            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private void storeDocument(String url, String text) {
        Document doc = Jsoup.parse(text);
        if (doc == null) return;
        Element body = doc.body();
        if (body == null) return;
        String path = "crawled/";
        try (PrintWriter writer = new PrintWriter(path + url.replace('/', '_') + ".txt", "UTF-8")) {
            writer.print(body);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private List<String> parseUrls(String text) {
        Document doc = Jsoup.parse(text);
        if (doc == null) return new ArrayList<>();
        Element body = doc.body();
        if (body == null) return new ArrayList<>();
        Elements links = body.select("a");
        if (links == null) return new ArrayList<>();
        List<String> urls = links.eachAttr("abs:href");
        return urls;
    }
}
