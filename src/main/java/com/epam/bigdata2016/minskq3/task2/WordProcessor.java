package com.epam.bigdata2016.minskq3.task2;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.*;

public class WordProcessor {

    private List<String> stopWords = Arrays.asList("a", "and", "for", "to", "the", "you", "in");

    public static void main(String[] args) throws Exception {
        final String inputPath = args[0];
        final int startN = Integer.valueOf(args[1]);
        final int endN = Integer.valueOf(args[2]);
        WordProcessor wp = new WordProcessor();
        wp.run(inputPath, startN, endN);
    }

    public void run(String inputPath, int startN, int endN) {
        System.out.println("PARAMS");
        System.out.println(inputPath + "_" + startN + "_" + endN);
        try {
            List<String> lines = HDFSHelper.readLines(new Path(Constants.HDFS_ROOT_PATH + inputPath));
            List<List<String>> totalTopWords = new ArrayList<>();
            List<String> resultLines = new ArrayList<>();

            if (startN == 0) {
                startN = 1;
            }
            for (int i = startN; i < endN; ++i) {
                String line = lines.get(i);
                System.out.println("line:" + line);
                String link = WordProcessorHelper.extractLink(line);
                System.out.println("link:" + link);
                String text = WordProcessorHelper.extractTextFromUrl(link);
                System.out.println("text:" + text);

                List<String> words = Pattern.compile("\\W").splitAsStream(text)
                        .filter((s -> !s.isEmpty()))
                        .filter(w -> !Pattern.compile("\\d+").matcher(w).matches())
                        .filter(w -> !stopWords.contains(w))
                        .collect(toList());

                List<String> topWords = words.stream()
                        .map(String::toLowerCase)
                        .collect(groupingBy(Function.identity(), counting()))
                        .entrySet().stream()
                        .sorted(Map.Entry.<String, Long>comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
                        .limit(10)
                        .map(Map.Entry::getKey)
                        .collect(toList());
                totalTopWords.add(topWords);
            }

            for (int i = startN; i < endN; ++i) {
                String currentLine = lines.get(i);
                String totalWords = "";
                List<String> words = totalTopWords.get(i);
                for (int j = 0; j < words.size(); j++) {
                    if (j > 0) {
                        totalWords += ",";
                    }
                    totalWords += words.get(j);
                }
                String resultText = currentLine.replaceFirst("\\s", " " + totalWords);
                System.out.println("result text: " + resultText);
                resultLines.add(resultText);
            }
            System.out.println("COUNT !!! " + resultLines.size());
            StringBuilder sb = new StringBuilder();
            int index = inputPath.lastIndexOf('/');
            String outpurPath = inputPath.substring(0, index);
            sb.append(outpurPath);
            sb.append("tasl2_lines_");
            sb.append(startN);
            sb.append("_");
            sb.append(endN);
            sb.append(".txt");

            System.out.println("Start writing 0 " + sb.toString());
            HDFSHelper.writeLines(resultLines, sb.toString());

        } catch (Exception e) {
            System.out.println("Error");
            System.out.println(e.getMessage());
        }
    }
}
