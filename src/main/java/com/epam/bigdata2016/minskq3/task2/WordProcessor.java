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

    private List<String> stopWords = Arrays.asList("a", "and", "to", "the", "in", "badword1", "badword2");

    public static void main(String[] args) throws Exception {
        final String inputPath = args[0];
        final int startN = Integer.valueOf(args[1]);
        final int endN = Integer.valueOf(args[2]);
        WordProcessor wp = new WordProcessor();
        wp.run(inputPath, startN, endN);
    }

    public void run(String inputPath, int startN, int endN) {
        try {
            List<String> lines = HDFSHelper.readLines(new Path(Constants.HDFS_ROOT_PATH + inputPath));
            List<List<String>> totalTopWords = new ArrayList<>();
            List<String> resultLines = new ArrayList<>();
            List<String> currentLines = new ArrayList<>();

            if (startN == 0) {
                startN = 1;
            }
            for (int i = startN; i < endN; ++i) {
                String line = lines.get(i);
                currentLines.add(line);
                String link = WordProcessorHelper.extractLink(line);
                String text = WordProcessorHelper.extractTextFromUrl(link);

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

            resultLines.add(lines.get(0));
            for (int i = 0; i < currentLines.size(); ++i) {
                String currentLine = currentLines.get(i);
                String totalWords = "";
                List<String> words = totalTopWords.get(i);
                for (int j = 0; j < words.size(); j++) {
                    if (j > 0) {
                        totalWords += ",";
                    }
                    totalWords += words.get(j);
                }
                String resultText = currentLine.replaceFirst("\\s", " " + totalWords);
                resultLines.add(resultText);
            }

            StringBuilder sb = new StringBuilder();
            int index = inputPath.lastIndexOf('/');
            String outpurPath = inputPath.substring(0, index);
            sb.append(outpurPath);
            sb.append("/task2_lines_");
            sb.append(startN);
            sb.append("_");
            sb.append(endN);
            sb.append(".txt");

            HDFSHelper.writeLines(resultLines, sb.toString());

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
