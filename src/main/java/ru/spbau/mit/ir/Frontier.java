package ru.spbau.mit.ir;

import java.util.ArrayDeque;
import java.util.Queue;

public class Frontier {

    private Queue<String> queue;

    public Frontier() {
        queue = new ArrayDeque<String>();
    }

    public boolean done() {
        return queue.isEmpty();
    }

    public Website nextSite() {
        return new Website(queue.poll());
    }

    public void addUrl(String url) {
        queue.add(url);
    }

    public void releaseSite(Website website) {

    }
}
