package com.epam.bigdata2016.minskq3.task2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class WordProcessorHelper {

    private static String urlPatternString = "http[s]*:[^\\s\\r\\n]+";

    public static String extractLink(String textLine) {
        String link = "";
        Pattern urlPattern = Pattern.compile(urlPatternString);
        Matcher urlMatcher = urlPattern.matcher(textLine);
        urlMatcher.matches();
        while (urlMatcher.find()) {
            link = urlMatcher.group();
        }
        return link;
    }

    public static String extractTextFromUrl(String url) {
        Document doc;
        try {
            StringBuilder sb = new StringBuilder();
            if (null != url && !url.isEmpty()) {
                doc = Jsoup.connect(url).timeout(5000).get();
                Elements children = doc.body().children();
                for (Element child : children) {
                    String text = child.text();
                    if (!text.contains("div")) {
                        sb.append(text);
                        sb.append(" ");
                    } else {
                        text = Jsoup.parse(child.outerHtml()).text();
                        if (!text.contains("div")) {
                            sb.append(text);
                            sb.append(" ");
                        }
                    }
                }
            }
            return sb.toString();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            String words = extractWordsFromUrl(url);
            return words;
        }
    }

    public static String extractWordsFromUrl(String link) {
        return link.split(".com/")[1].replace(".html", "").replace("-", " ");
    }
}