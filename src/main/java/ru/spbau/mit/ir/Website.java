package ru.spbau.mit.ir;

public class Website {
    private String rootPage;
    private Robots robots;

    public Website(String rootPage) {
        this.rootPage = rootPage;
        this.robots = new Robots(rootPage);
    }

    public String nextUrl() {
        return rootPage;
    }

    public boolean permitsCrawl(String url) {
        return robots.permit(url);
    }
}
