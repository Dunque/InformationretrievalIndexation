package es.udc.fi.ri.mipractica;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.IndexableField;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ManualRelevanceFeedbackNPL {
	
	static String index = "index";
	static int cut = 10;
	static int metrica = 0; //0 = P, 1 = R, 2 = MAP
	static String residual = "T";
	static String queryNum = "1";
	static HashMap<Integer, String> queries = new HashMap<>();
	static String indexingmodel = "tfidf";
	static float lambda = 0.5f;
    static float mu = 0.5f;
    static String field = "contents";
    static final Path QUERIES_PATH = Paths.get("/home/anton/Desktop/RI/p2/InformationretrievalIndexation/npl/query-text");
    static final Path RELEVANCE_PATH = Paths.get("/home/anton/Desktop/RI/p2/InformationretrievalIndexation/npl/rlv-ass");
    static Path queryFile = QUERIES_PATH;
    
	private static void parseArguments(String[] args) {
		
        String usage = "java -jar ManualRelevanceFeedbackNPL-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-retmodel <jm lambda | dir mu | tfidf>][-indexin pathname] [-cut n] "
                + " [-metrica <P | R | MAP>] [-residual <T | F>] [-query q]";

        if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
            System.out.println(usage);
            System.exit(0);
        }

        for (int i = 0; i < args.length; i++) {
        	if ("-retmodel".equals(args[i])) {
                if (args[i + 1].equals("jm")) {
                    indexingmodel = args[i + 1];
                    lambda = Float.parseFloat(args[i + 2]);
                    i += 2;
                } else if (args[i + 1].equals("dir")) {
                    indexingmodel = args[i + 1];
                    mu = Float.parseFloat(args[i + 2]);
                    i += 2;
                } else if (args[i + 1].equals("tfidf")) {
                    indexingmodel = args[i + 1];
                    i++;
                } else
                    System.out.println("Error reading Retrieval Model, defaulting to tfidf");
            }else if ("-indexin".equals(args[i])) {
                index = args[i + 1];
                i++;
            }else if ("-cut".equals(args[i])) {
                cut = Integer.parseInt(args[i + 1]);
                if (cut <= 0) {
                    System.err.println("There must be at least 1 hit per page.");
                    System.exit(1);
                }
                i++;
            }else if ("-metrica".equals(args[i])) {
                if (args[i + 1].equals("P")) {
                    metrica = 0;
                    i++;
                } else if (args[i + 1].equals("R")) {
                    metrica = 1;
                    i++;
                } else if (args[i + 1].equals("MAP")) {
                    metrica = 2;
                    i++;
                } else
                    System.out.println("Error, metric not recognized, defaulting to P");
            }else if ("-residual".equals(args[i])) {
                if (args[i + 1].equals("T") || args[i + 1].equals("F")) {
                    residual = args[i+1];
                    i++;
                } else
                    System.out.println("Error, residual not recognized, defaulting to T");
            }else if ("-query".equals(args[i])) {
            	queryNum = args[i + 1];
                i++;
            }
        	
        }
	}
	
	public static HashMap<Integer, String> findQuery(String n) throws IOException {
        try (InputStream stream = Files.newInputStream(queryFile)) {
            String line;
            HashMap<Integer, String> result = new HashMap<>();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            while ((line = br.readLine()) != null) {
                if (line.equals(n)){
                    result.put(Integer.parseInt(n), br.readLine());
                    break;
                }
            }
            br.close();
            stream.close();
            return result;
        }
    }
	
    public static void main( String[] args ) throws Exception
    {
    	parseArguments(args);
    	
    	IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();
        
        switch (indexingmodel) {
        	case "jm":
        		searcher.setSimilarity(new LMJelinekMercerSimilarity(lambda));
        		break;
        	case "dir":
        		searcher.setSimilarity(new LMDirichletSimilarity(mu));
        		break;
        	case "tfidf":
        		searcher.setSimilarity(new ClassicSimilarity());
        		break;
        	default:
        		searcher.setSimilarity(new ClassicSimilarity());
        		break;
        }
        
        queries.putAll(findQuery(queryNum));
        
        QueryParser parser = new QueryParser(field, analyzer);
        for (Map.Entry<Integer, String> entry : queries.entrySet()) {
        	int num = entry.getKey();
            String line = entry.getValue();
            line = line.trim();
            Query query = parser.parse(line);
            System.out.println("Searching for: " + query.toString(field));
            doPagingSearch(searcher, query, num);
        }
        
    }
    
    public static List<Integer> findRelevantDocs(Path file, int query) throws IOException {

        List<Integer> result = new ArrayList<>();
        try (InputStream stream = Files.newInputStream(file)) {
            String line;
            BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            while ((line = br.readLine()) != null) {
                try {
                    int num = Integer.parseInt(line);

                    if (num == query) {
                        String line2;
                        String[] aux;
                        while ((line2 = br.readLine()) != null) {
                            if (line2 == null || line2.trim().equals("/"))
                                break;
                            aux = line2.split("\\s+");
                            for (String str : aux) {
                                int num2;
                                try {
                                    num2 = Integer.parseInt(str);
                                    result.add(num2);
                                } catch (NumberFormatException e) {
                                }
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                }
            }
            return result;
        }
    }
    
    public static void doPagingSearch(IndexSearcher searcher, Query query, int num) throws IOException {
    	TopDocs results = searcher.search(query, cut);
    	ScoreDoc[] hits = results.scoreDocs;
        List<Integer> relevantDocs = findRelevantDocs(RELEVANCE_PATH, num);
        Set<Integer> relevantSet = new HashSet<>();
        List<Float> accumPrecision = new ArrayList<>();
        
        int numTotalHits = Math.toIntExact(results.totalHits.value);
      //this loop is used for calculating the metrics
        int end = Math.min(numTotalHits, cut);
        int n = 1;
        for (int i = 0; i < end; i++) {
            Document doc = searcher.doc(hits[i].doc);
            int id = Integer.parseInt(doc.get("DocIDNPL"));
            for (int idaux : relevantDocs) {
                if (id == idaux) {
                    relevantSet.add(id);
                    float prec = (float) relevantSet.size() / n;
                    accumPrecision.add(prec);
                }
            }
            n++;
        }
        
        switch (metrica) {
        	case 0:
        		System.out.println("Precision at " + cut + " = " + (float) relevantSet.size() / cut);
        		break;
        		
        	case 1:
        		System.out.println("Recall at " + cut + " = " + (float) relevantSet.size() / relevantDocs.size());
        		break;

        	case 2: {
        		if (relevantSet.size() != 0) {
        			float sum = 0;
        			for (Float d : accumPrecision)
        				sum += d;
        			System.out.println("Mean Average Precision at " + cut + " = " + (float) sum / relevantSet.size());
        		} else
        			System.out.println("Can't compute Mean Average Precision at " + cut + ", no relevant documents found");
        		break;
        	}

        	default:
        		System.out.println("Precision at " + cut + " = " + (float) relevantSet.size() / cut);
        		break;
        }
        Document doc = searcher.doc(hits[0].doc);
        int id = Integer.parseInt(doc.get("DocIDNPL"));
        System.out.println("Documento más relevante: ID: " + id + " score = " + hits[0].score);
        System.out.println();
        
        List<IndexableField> fields = null;
        
        fields = doc.getFields();
		for (IndexableField field : fields) {
			String fieldName = field.name();
			System.out.println(fieldName + ": " + doc.get(fieldName));

		}
    }
}
