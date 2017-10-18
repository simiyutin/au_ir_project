package ru.spbau.mit.ir.crawler.crawler2;

import java.net.MalformedURLException;
import java.net.URL;

public class Website {
    public static String getHost(String url) {
        try {
            URL u = new URL(url);
            String result = u.getHost();
            return result;
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String getProtocol(String url) {
        try {
            URL u = new URL(url);
            String result = u.getProtocol();
            return result;
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        }
    }

}
