package es.udc.fi.ri.mipractica;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IndexNPL {

    static final String DEFAULT_PATH = "src/main/resources/config.properties";

    static String indexPath = "index"; //default index path is a folder named index located in the root dir
    static Path docPath;
    static String indexingmodel = "jm";
    static float lambda = 0.5f;
    static float mu = 0.5f;
    static IndexWriterConfig.OpenMode openmode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND;

    private IndexNPL() {
    }

    private static void parseArguments(String[] args) {

        String usage = "java -jar IndexNPL-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index INDEX_PATH] [-openmode <APPEND | CREATE | APPEND_OR_CREATE>]\n";

        if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
            System.out.println(usage);
            System.exit(0);
        }

            for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-openmode".equals(args[i])) {
                openmode = IndexWriterConfig.OpenMode.valueOf(args[i + 1]);
                System.out.println(args[i] + args[i + 1]);
                i++;
            }
        }
    }

    private static void readConfigFile(String path) {

        FileInputStream inputStream;
        Properties prop = new Properties();

        try {
            inputStream = new FileInputStream(path);
            prop.load(inputStream);
        } catch (IOException ex) {
            System.out.println("Error reading config file: " + ex);
            System.exit(-1);
        }

        //Read and store doc path
        String docsList = prop.getProperty("docs");
        if (docsList != null) {
            String[] docsSplit = docsList.split(" ");
            docPath = Paths.get(docsSplit[0]);

        } else {
            System.out.println("Error in the config file, there is no doc path");
            System.exit(-1);
        }

        //Read and store doc path
        String im = prop.getProperty("indexingmodel");
        if (im != null) {
            String[] imsplit = im.split(" ");
            if (imsplit.length == 2){
                indexingmodel = imsplit[0];
                if (imsplit[0].equals("jm"))
                    lambda = Float.parseFloat(imsplit[1]);
                else if (imsplit[0].equals("dir"))
                    mu = Float.parseFloat(imsplit[1]);
                else
                    System.out.println("Error reading Indexing Model, defaulting to jm 0.5");
            } else if (imsplit.length == 1 && imsplit[0].equals("tfidf"))
                indexingmodel = imsplit[0];
            else
                System.out.println("Error reading Indexing Model, defaulting to jm 0.5");
        } else {
            System.out.println("Error reading Indexing Model, defaulting to jm 0.5");
        }
    }

    static void indexDoc(IndexWriter writer, Path file) throws IOException {

        try (InputStream stream = Files.newInputStream(file)) {
            String line;
            BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            while ((line = br.readLine()) != null){
                int num;
                try {
                    num = Integer.parseInt(line);
                    String contents = "";
                    Document doc = new Document();

                    String line2;
                    while((line2 = br.readLine()) != null){
                        if (line2 == null || line2.trim().equals("/"))
                            break;
                        contents += line2 + "\n";
                    }

                    doc.add(new StringField("DocIDNPL", String.valueOf(num), Field.Store.YES));

                    doc.add(new StringField("contents", contents, Field.Store.YES));

                    if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                        writer.addDocument(doc);
                    } else {
                        writer.updateDocument(new Term("path", file.toString()), doc);
                    }
                }
                catch (NumberFormatException e)
                {
                }
            }
        }

    }

    public static void main(String[] args) {

        parseArguments(args);
        readConfigFile(DEFAULT_PATH);

        if (!Files.isReadable(docPath)) {
            System.out.println("Document directory '" + docPath.toAbsolutePath()
                    + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            iwc.setOpenMode(openmode);

            switch (indexingmodel){
                case "jm": iwc.setSimilarity(new LMJelinekMercerSimilarity(lambda));
                case "dir": iwc.setSimilarity(new LMDirichletSimilarity(mu));
                case "tfidf": iwc.setSimilarity(new ClassicSimilarity());
            }

            System.out.println(indexingmodel);
            System.out.println(lambda);
            System.out.println(mu);

            IndexWriter writer = new IndexWriter(dir, iwc);

            indexDoc(writer, docPath);

            try {
                writer.commit();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " total milliseconds");

        } catch (IOException e) {
            System.out.println("Caught a " + e.getClass() + " with message: " + e.getMessage());
        }
    }
}
