package es.udc.fi.ri.mipractica;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SearchEvalNPL {

    static String index = "index";
    static String field = "contents";
    static Path queryFile = Paths.get("npl/query-text");
    static String indexingmodel = "tfidf";
    static float lambda = 0.5f;
    static float mu = 0.5f;
    static int queryMode = 0; // 0 = all | int1-int2, 1 = int
    static String queryRange = "1-2";
    static String queryNum = "1";

    static List<String> queries = new ArrayList<>();
    static int repeat = 0;
    static String queryString = null;
    static int hitsPerPage = 10;

    private SearchEvalNPL() {
    }

    private static void parseArguments(String[] args) {

        String usage = "java -jar SearchEvalNPL-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index dir] [-search <jm lambda | dir mu | tfidf>]"
                + " [-repeat n] [-queries <all | int | int1-int2>]"
                + " [-query string] [-paging hitsPerPage]\n";

        if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
            System.out.println(usage);
            System.exit(0);
        }

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                index = args[i + 1];
                i++;
            } else if ("-search".equals(args[i])) {
                if (args[i + 1].equals("jm")) {
                    lambda = Float.parseFloat(args[i + 2]);
                    i += 2;
                } else if (args[i + 1].equals("dir")) {
                    mu = Float.parseFloat(args[i + 2]);
                    i += 2;
                } else if (args[i + 1].equals("tfidf")) {
                    indexingmodel = args[i + 1];
                    i++;
                } else
                    System.out.println("Error reading Indexing Model, defaulting to tfidf");
            } else if ("-queries".equals(args[i])) {
                if (args[i + 1].equals("all")) {
                    queryMode = 0;
                    queryRange = "1-93";
                    i++;
                } else if (args[i + 1].contains("-")) {
                    queryMode = 0;
                    queryRange = args[i + 1];
                    i++;
                } else {
                    queryMode = 1;
                    queryNum = args[i + 1];
                    i++;
                }
            } else if ("-repeat".equals(args[i])) {
                repeat = Integer.parseInt(args[i + 1]);
                i++;
            } else if ("-paging".equals(args[i])) {
                hitsPerPage = Integer.parseInt(args[i + 1]);
                if (hitsPerPage <= 0) {
                    System.err.println("There must be at least 1 hit per page.");
                    System.exit(1);
                }
                i++;
            }
        }
    }

    public static String findQuery(String n) throws IOException {

        try (InputStream stream = Files.newInputStream(queryFile)) {
            String line;
            BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            while ((line = br.readLine()) != null) {
                if (line.equals(n))
                    return br.readLine();
            }
            return null;
        }
    }

    public static List<String> findQueries(String range) throws IOException {

        List<String> result = new ArrayList<>();
        String nums[] = range.split("-");

        if (nums.length != 2) {
            System.err.println("Query range is in an incorrect format; it must be Int1-Int2");
            System.exit(1);
        }

        int top = Integer.parseInt(nums[0]);
        int bot = Integer.parseInt(nums[1]);

        for (int i = top; i <= bot; i++) {
            result.add(findQuery(String.valueOf(i)));
        }
        return result;
    }

    public static void main(String[] args) throws Exception {

        parseArguments(args);

        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();

        switch (indexingmodel) {
            case "jm":
                searcher.setSimilarity(new LMJelinekMercerSimilarity(lambda));
            case "dir":
                searcher.setSimilarity(new LMDirichletSimilarity(mu));
            case "tfidf":
                searcher.setSimilarity(new ClassicSimilarity());
            default:
                searcher.setSimilarity(new ClassicSimilarity());
        }

        //BufferedReader in = null;

        switch (queryMode) {
            case 0:
                queries.addAll(findQueries(queryRange));
            case 1:
                queries.add(findQuery(queryNum));
        }

        QueryParser parser = new QueryParser(field, analyzer);


        for (String line : queries) {
            line = line.trim();
            Query query = parser.parse(line);
            System.out.println("Searching for: " + query.toString(field));
            doPagingSearch(searcher, query, hitsPerPage);
        }


        reader.close();
    }

    public static void doPagingSearch(IndexSearcher searcher, Query query, int hitsPerPage) throws IOException {

        // Collect enough docs to show 5 pages
        TopDocs results = searcher.search(query, 5 * hitsPerPage);
        ScoreDoc[] hits = results.scoreDocs;

        int numTotalHits = Math.toIntExact(results.totalHits.value);
        System.out.println(numTotalHits + " total matching documents");

        int start = 0;
        int end = Math.min(numTotalHits, hitsPerPage);

        for (int i = start; i < end; i++) {

            Document doc = searcher.doc(hits[i].doc);
            String id = doc.get("DocIDNPL");
            if (id != null) {
                System.out.println((i + 1) + ". Doc ID: " + id + " score = " + hits[i].score);
            } else {
                System.out.println((i + 1) + ". " + "No id for this document");
            }

        }


    }
}
