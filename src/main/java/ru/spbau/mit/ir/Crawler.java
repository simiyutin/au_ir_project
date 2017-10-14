package ru.spbau.mit.ir;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Crawler {

    private Frontier frontier;
    private Set<Integer> visitedPages;

    public Crawler(String initialUrl) {
        frontier = new Frontier();
        frontier.addUrl(initialUrl);
        visitedPages = new HashSet<Integer>();
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

            if (website.permitsCrawl(url)) {
                String html = retrieveUrl(url);
                if (html != null) {
                    storeDocument(url, html);
                    List<String> nestedUrls = parseUrls(html);
                    for (String nestedUrl : nestedUrls) {
                        frontier.addUrl(nestedUrl);
                    }
                }
            }
            frontier.releaseSite(website);

            try {
                TimeUnit.MILLISECONDS.sleep(700);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private String retrieveUrl(String targetURL) {
        HttpURLConnection connection = null;

        try {
            URL url = new URL(targetURL);
            connection = (HttpURLConnection) url.openConnection();
            connection.addRequestProperty("User-Agent", "spbauCrawler");

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
        String bodyText = body.text();
    }

    private List<String> parseUrls(String text) {
        Document doc = Jsoup.parse(text);
        Element body = doc.body();
        System.out.println(body.text());
        Elements links = body.select("a");
        List<String> urls = links.eachAttr("abs:href");
        System.out.println(urls);
        return urls;
    }
}
