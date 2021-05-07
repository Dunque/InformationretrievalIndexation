package es.udc.fi.ri.mipractica;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
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

    private static class WorkerThread extends Thread {

        private final List<Path> folders;
        private IndexWriter writer;

        //Used in IndexNonPartial
        public WorkerThread(final List<Path> folders, IndexWriter writer) {
            this.folders = folders;
            this.writer = writer;
        }

        @Override
        public void run() {
            String ThreadName = Thread.currentThread().getName();

            for (Path path : folders) {

                System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
                        Thread.currentThread().getName(), path));
                try {
                    System.out.println(ThreadName + ": Indexing to directory '" + path + "'...");
                    indexDocs(writer, path);
                } catch (IOException e) {
                    System.out.println(ThreadName + ": caught a " + e.getClass() + "with message: " + e.getMessage());
                }
            }
        }

    }

    static final String DEFAULT_PATH = "src/main/resources/config.properties";

    static String indexPath = "index"; //default index path is a folder named index located in the root dir
    static List<Path> docsPath = new ArrayList<>();
    static IndexWriterConfig.OpenMode openmode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
    static int numThreads = Runtime.getRuntime().availableProcessors();

    private IndexNPL() {
    }

    private static void parseArguments(String[] args) {

        String usage = "java -jar IndexNPL-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index INDEX_PATH] [-openmode <APPEND | CREATE | APPEND_OR_CREATE>]"
                + " [-numThreads NUM_THREADS]\n";

        if (args.length < 1)
            System.out.println(usage);

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-openmode".equals(args[i])) {
                openmode = IndexWriterConfig.OpenMode.valueOf(args[i + 1]);
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-numThreads".equals(args[i])) {
                numThreads = Integer.parseInt(args[i + 1]);
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

        //Read and store docs paths
        String docsList = prop.getProperty("docs");
        if (docsList != null) {
            String[] docsSplit = docsList.split(" ");
            for (int i = 0; i < docsSplit.length; i++) {
                Path doc = Paths.get(docsSplit[i]);
                docsPath.add(doc);
            }
        } else {
            System.out.println("Error in the config file, there are no docs paths");
            System.exit(-1);
        }
    }

    static void indexDocs(final IndexWriter writer, Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    try {
                        indexDoc(writer, file);
                    } catch (IOException ignore) {
                        // don't index files that can't be read.
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            indexDoc(writer, path);
        }
    }

    public static final FieldType TYPE_STORED = new FieldType();

    static final IndexOptions options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;

    static {
        TYPE_STORED.setIndexOptions(options);
        TYPE_STORED.setTokenized(true);
        TYPE_STORED.setStored(true);
        TYPE_STORED.setStoreTermVectors(true);
        TYPE_STORED.setStoreTermVectorPositions(true);
        TYPE_STORED.freeze();
    }

    static void indexDoc(IndexWriter writer, Path file) throws IOException {

        try (InputStream stream = Files.newInputStream(file)) {

            Document doc = new Document();

            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            doc.add(pathField);

            doc.add(new TextField("contents",
                    new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

            doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
            doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));
            doc.add(new DoublePoint("sizeKb", (double) Files.size(file)));
            //doc.add(new StoredField("sizeKb", (double) Files.size(file), ));

            BasicFileAttributeView basicView = Files.getFileAttributeView(file, BasicFileAttributeView.class);

            String creationTime = basicView.readAttributes().creationTime().toString();
            String lastAccessTime = basicView.readAttributes().lastAccessTime().toString();
            String lastModifiedTime = basicView.readAttributes().lastModifiedTime().toString();
            doc.add(new StringField("creationTime", creationTime, Field.Store.YES));
            doc.add(new StringField("lastAccessTime", lastAccessTime, Field.Store.YES));
            doc.add(new StringField("lastModifiedTime", lastModifiedTime, Field.Store.YES));

            String creationTimeLucene = DateTools.dateToString(new Date(basicView.readAttributes().creationTime().toMillis()), DateTools.Resolution.MINUTE);
            String lastAccessTimeLucene = DateTools.dateToString(new Date(basicView.readAttributes().lastAccessTime().toMillis()), DateTools.Resolution.MINUTE);
            String lastModifiedTimeLucene = DateTools.dateToString(new Date(basicView.readAttributes().lastModifiedTime().toMillis()), DateTools.Resolution.MINUTE);
            doc.add(new StringField("creationTimeLucene", creationTimeLucene, Field.Store.YES));
            doc.add(new StringField("lastAccessTimeLucene", lastAccessTimeLucene, Field.Store.YES));
            doc.add(new StringField("lastModifiedTimeLucene", lastModifiedTimeLucene, Field.Store.YES));

            if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                System.out.println(Thread.currentThread().getName() + " adding " + file);
                writer.addDocument(doc);
            } else {
                System.out.println(Thread.currentThread().getName() + " updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }

    }

    public static void indexNonPartial(IndexWriter writer) {
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        List<Path> docsPathAux = new ArrayList<>(docsPath);
        ArrayList<Path>[] al = new ArrayList[numThreads];
        for (int i = 0; i < numThreads; i++) {
            al[i] = new ArrayList<>();
        }

        while (!docsPathAux.isEmpty()) {
            for (int i = 0; i < numThreads; i++) {
                if (docsPathAux.size() > 0 && docsPathAux.get(0) != null) {
                    al[i].add(docsPathAux.get(0));
                    docsPathAux.remove(0);
                }
            }
        }

        for (int i = 0; i < numThreads; i++) {
            final Runnable worker = new WorkerThread(al[i], writer);
            executor.execute(worker);
        }

        executor.shutdown();
        /* Wait up to 1 hour to finish all the previously submitted jobs */
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }
    }

    public static void main(String[] args) {

        parseArguments(args);
        readConfigFile(DEFAULT_PATH);

        for (Path path : docsPath) {
            if (!Files.isReadable(path)) {
                System.out.println("Document directory '" + path.toAbsolutePath()
                        + "' does not exist or is not readable, please check the path");
                System.exit(1);
            }
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            iwc.setOpenMode(openmode);

            IndexWriter writer = new IndexWriter(dir, iwc);

            indexNonPartial(writer);

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
