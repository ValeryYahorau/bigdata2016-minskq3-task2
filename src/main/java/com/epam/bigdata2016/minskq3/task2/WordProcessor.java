package com.epam.bigdata2016.minskq3.task2;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * Created by valeryyegorov on 30.08.16.
 */
public class WordProcessor {

    public void run(String[] args) {




        try{
            Pattern p = Pattern.compile("http://www.miniinthebox.com[^\\n]*");
            List<String> urls = new ArrayList<String>();

            Path pt=new Path("hdfs://sandbox.hortonworks.com:8020/tmp/admin/user.profile.tags.us.min.txt");

            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            FileSystem fs = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"),conf);
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            List<String> lines = new ArrayList<String>();

            String line=br.readLine();
            String topLine = line;
            line=br.readLine();
            while (line != null){
                lines.add(line.trim());
                line=br.readLine();
            }

            System.out.println("WP STEP 1 " + lines.size());
            for (String l : lines) {
                Matcher m = p.matcher(l);
                m.matches();
                while (m.find()) {
                    urls.add(m.group());
                }
            }

            System.out.println("WP STEP 2 " +urls.size());
            List<List<String>> totalTopWords = new ArrayList<>();
            for (String u : urls) {
                System.out.println("WP STEP 2 URL :  " +u);
                Document d = Jsoup.connect(u).timeout(0).get();
                String text = d.body().text();

                StringTokenizer tokenizer = new StringTokenizer(text, " .,?!:;()<>[]\b\t\n\f\r\"\'\\");
                List<String> words = new ArrayList<String>();
                while(tokenizer.hasMoreTokens()) {
                    words.add(tokenizer.nextToken());
                    //System.out.println(tokenizer.nextToken());
                }

                List<String> topWords = words.stream()
                        .map(String::toLowerCase)
                        .collect(groupingBy(Function.identity(), counting()))
                        .entrySet().stream()
                        .sorted(Map.Entry.<String, Long> comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
                        .limit(10)
                        .map(Map.Entry::getKey)
                        .collect(toList());
                totalTopWords.add(topWords);
            }

            System.out.println("WP STEP 3 " +totalTopWords.size());
            try{
                Path ptOut=new Path("hdfs://sandbox.hortonworks.com:8020/tmp/admin/user.profile.tags.us.min.out.txt");
                Configuration confOut = new Configuration();
                confOut.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
                confOut.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

                FileSystem fsOut = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"),confOut);

                BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(fsOut.create(ptOut,true)));

                brOut.write(topLine);
                brOut.write("\n");
                System.out.println("WP STEP 4");
                for (int i = 0; i < lines.size(); i++) {
                    String currentLine = lines.get(i);
                    String[] params = currentLine.split("\\s+");
                    for (int j = 0; j < params.length; j++) {
                        if (j == 1) {
                            List<String> currentTopWords = totalTopWords.get(i);

                            for (int k = 0; k < currentTopWords.size(); k++) {
                                brOut.write(currentTopWords.get(k));
                                if (k < (currentTopWords.size()-1)) {
                                    brOut.write(",");
                                }
                            }
                            brOut.write(" ");
                        }
                        brOut.write(params[j]);
                        if (j < (params.length-1)) {
                            brOut.write(" ");
                        }
                    }
                    brOut.write("\n");
                }

                System.out.println("WP STEP 5");
                brOut.close();
            }catch(Exception e) {
                System.out.println(e.getMessage());
            }
        }catch(Exception e){
            System.out.println(e.getMessage());
        }

    }

    public static void main(String[] args) throws Exception {
        WordProcessor wp = new WordProcessor();
        wp.run(args);
    }
}
