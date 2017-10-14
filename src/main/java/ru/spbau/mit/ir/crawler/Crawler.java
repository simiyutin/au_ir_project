package ru.spbau.mit.ir.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Crawler {

    private Frontier frontier;
    private Set<Integer> visitedPages;
    private final String userAgent = "spbauCrawler";
    private int processed;
    private Map<String, Long> lastVisitTimes;
    private final long coolDownTime = 500;

    public Crawler(String initialUrl) {
        frontier = new Frontier();
        frontier.addUrl(initialUrl);
        visitedPages = new HashSet<>();
        processed = 0;
        lastVisitTimes = new HashMap<>();
    }

    public void crawl() {
        while (!frontier.done()) {
            Website website = frontier.nextSite();
            String url = website.nextUrl();
            int hash = url.hashCode();
            if (visitedPages.contains(hash)) {
                continue;
            }
            visitedPages.add(hash);

            coolDown(url);

            if (website.permitsCrawl(userAgent, url)) {
                String html = retrieveUrl(url);
                if (html != null) {
                    storeDocument(url, html);
                    List<String> nestedUrls = parseUrls(html);
                    System.out.println(nestedUrls);
                    for (String nestedUrl : nestedUrls) {
                        frontier.addUrl(nestedUrl);
                    }

                }
            }
            frontier.releaseSite(website);
            processed++;
            System.out.println(String.format("queue size:%s, processed:%s", frontier.size(), processed));
        }

    }

    private void coolDown(String url) {
        try {
            String host = new URL(url).getHost();
            if (lastVisitTimes.containsKey(host)) {
                long lastVisitTime = lastVisitTimes.get(host);
                long curTime = System.currentTimeMillis();
                long delta = curTime - lastVisitTime;
                if (delta < coolDownTime) {
                    TimeUnit.MILLISECONDS.sleep(coolDownTime - delta);
                }
            }
            lastVisitTimes.put(host, System.currentTimeMillis());
        } catch (MalformedURLException | InterruptedException e) {
            e.printStackTrace();
        }
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
                        || status == HttpURLConnection.HTTP_SEE_OTHER)
                    redirect = true;
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
        Element body = doc.body();
        String path = "crawled/";
        try (PrintWriter writer = new PrintWriter(path + url.replace('/', '_') + ".txt", "UTF-8")) {
            writer.print(body);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private List<String> parseUrls(String text) {
        Document doc = Jsoup.parse(text);
        Element body = doc.body();
        Elements links = body.select("a");
        List<String> urls = links.eachAttr("abs:href");
        return urls;
    }
}
