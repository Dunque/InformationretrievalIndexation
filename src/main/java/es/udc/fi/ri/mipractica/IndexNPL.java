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


    public static class WorkerThread2 implements Runnable {

        private final Path folder;
        private final FSDirectory dir;

        public WorkerThread2(final Path folder, final FSDirectory dir) {
            this.folder = folder;
            this.dir = dir;
        }

        @Override
        public void run() {
            try {
                Analyzer analyzer = new StandardAnalyzer();
                IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
                iwc.setOpenMode(openmode);
                IndexWriter writer = new IndexWriter(dir, iwc);

                Files.walkFileTree(folder, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attr) throws IOException {
                        try {
                            indexDoc(writer, file);
                        } catch (IOException ignore) {
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
                writer.close();

            } catch (IOException e) {
                System.out.println("Caught a " + e.getClass() + "with message: " + e.getMessage());
            }
        }

    }

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
    static boolean create = true; //Create true == Update false
    static boolean onlyFiles = false;
    static List<String> fileTypes = new ArrayList<>();
    static List<Path> docsPath = new ArrayList<>();
    static IndexWriterConfig.OpenMode openmode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
    static boolean partialIndex = false;
    static List<Path> partialIndexes = new ArrayList<>();
    static int numThreads = Runtime.getRuntime().availableProcessors();

    static int topLines = 0;
    static int bottomLines = 0;
    //final ExecutorService executor = Executors.newFixedThreadPool(numCores);

    private IndexNPL() {
    }

    private static void parseArguments(String[] args) {

        String usage = "java -jar IndexFiles-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index INDEX_PATH] [-update] [-onlyFiles]"
                + " [-openmode <APPEND | CREATE | APPEND_OR_CREATE>]"
                + " [-partialIndexes] [-numThreads NUM_THREADS]\n";

        if (args.length < 1)
            System.out.println(usage);

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-update".equals(args[i])) {
                create = false;
                System.out.println(args[i]);
            } else if ("-onlyFiles".equals(args[i])) {
                onlyFiles = true;
                System.out.println(args[i]);
            } else if ("-openmode".equals(args[i])) {
                openmode = IndexWriterConfig.OpenMode.valueOf(args[i + 1]);
                System.out.println(args[i] + args[i + 1]);
                i++;
            } else if ("-partialIndexes".equals(args[i])) {
                partialIndex = true;
                System.out.println(args[i]);
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

        //Read and store partial indexes paths
        String partIndexList = prop.getProperty("partialIndexes");
        if (partIndexList != null) {
            String[] partIndexSplit = partIndexList.split(" ");
            for (int i = 0; i < partIndexSplit.length; i++) {
                Path partIndex = Paths.get(partIndexSplit[i]);
                partialIndexes.add(partIndex);
            }
        } else {
            if (partialIndex)
                System.out.println("No partial index paths specified, creating new ones");
        }

        //Reading the allowed file types
        String fileTypesList = prop.getProperty("onlyFiles");
        if (fileTypesList != null) {
            String[] fileTypesSplit = fileTypesList.split(" ");
            fileTypes.addAll(Arrays.asList(fileTypesSplit));
        } else
            System.out.println("Warning, no file types specified in config file");

        //Reading topLines property
        String onlyTopLines = prop.getProperty("onlyTopLines");
        if (onlyTopLines != null) {
            try {
                topLines = Integer.parseInt(onlyTopLines);
            } catch (Exception e) {
                System.out.println("Error reading onlyTopLines property " + e);
            }
        }

        //Reading bottomLines property
        String onlyBottomLines = prop.getProperty("onlyBottomLines");
        if (onlyBottomLines != null) {
            try {
                bottomLines = Integer.parseInt(onlyBottomLines);
            } catch (Exception e) {
                System.out.println("Error reading onlyBottomLines property " + e);
            }
        }
    }

    private static String getExtension(File file) {
        String fileName = file.getName();
        if (fileName.contains("."))
            return fileName.substring(fileName.indexOf("."));
        return null;
    }

    private static String readFile(String file) throws IOException {

        String result = "";
        BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(file)));
        for (String line; (line = br.readLine()) != null; )
            result += line + "\n";

        br.close();

        return result;
    }

    private static String readNLines(String file, int ntop, int nbot, String mode) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader(file));
        List<String> wantedLines = new ArrayList<>();
        String result = "";


        for (String line; (line = br.readLine()) != null; )
            wantedLines.add(line);

        br.close();

        if (mode == "top") {
            for (int i = 0; i < ntop; i++)
                if (wantedLines.size() != 0) {
                    result += wantedLines.get(0) + "\n";
                    wantedLines.remove(0);
                }
            return result;

        } else if (mode == "bot") {
            int limit = wantedLines.size() - nbot;
            for (int i = wantedLines.size() - 1; i >= limit; i--)
                if (wantedLines.size() != 0) {
                    result += wantedLines.get(wantedLines.size() - 1) + "\n";
                    wantedLines.remove(wantedLines.size() - 1);
                }
            return result;

        } else {
            for (int i = 0; i < ntop; i++)
                if (wantedLines.size() != 0) {
                    result += wantedLines.get(0) + "\n";
                    wantedLines.remove(0);
                }
            int limit = wantedLines.size() - nbot;
            for (int i = wantedLines.size() - 1; i >= limit; i--)
                if (wantedLines.size() != 0) {
                    result += wantedLines.get(wantedLines.size() - 1) + "\n";
                    wantedLines.remove(wantedLines.size() - 1);
                }
            return result;
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

        if (fileTypes.isEmpty() || fileTypes.contains(getExtension(file.toFile()))) {
            try (InputStream stream = Files.newInputStream(file)) {

                Document doc = new Document();

                Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                doc.add(pathField);

                if (topLines != 0) {
                    if (bottomLines != 0)
                        doc.add(new Field("contents", readNLines(file.toString(), topLines, bottomLines, "topbot"), TYPE_STORED));
                    else
                        doc.add(new Field("contents", readNLines(file.toString(), topLines, bottomLines, "top"), TYPE_STORED));
                } else {
                    if (bottomLines != 0)
                        doc.add(new Field("contents", readNLines(file.toString(), topLines, bottomLines, "bot"), TYPE_STORED));
                    else
                        doc.add(new TextField("contents",
                                new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
                }

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
                    if (!create)
                        System.out.println("Warning, update in create mode is not possible");
                    // New index, so we just add the document (no old document can be there):
                    System.out.println(Thread.currentThread().getName() + " adding " + file);
                    writer.addDocument(doc);
                } else {
                    // Existing index (an old copy of this document may have been indexed) so
                    // we use updateDocument instead to replace the old one matching the exact
                    // path, if present:
                    if (create) {
                        System.out.println(Thread.currentThread().getName() + " adding " + file);
                        writer.addDocument(doc);
                    } else {
                        System.out.println(Thread.currentThread().getName() + " updating " + file);
                        writer.updateDocument(new Term("path", file.toString()), doc);
                    }

                }
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

    static void indexMulti(IndexWriter mainWriter) {
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<FSDirectory> directory_list = new ArrayList<>();

        try {
            int i = 0;
            for (Path p : docsPath) {
                Path partialIndexPath = partialIndexes.get(i);
                FSDirectory partialIndexDir = FSDirectory.open(partialIndexPath);
                directory_list.add(partialIndexDir);
                final Runnable worker = new WorkerThread2(p, partialIndexDir);
                executor.execute(worker);
                i++;
            }
        } catch (Exception e) {
            System.out.println("Caught a " + e.getClass() + "with message: " + e.getMessage());
        }
        executor.shutdown();

        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            System.out.println("Caught a " + e.getClass() + "with message: " + e.getMessage());
            System.exit(-2);
        } finally {
            System.out.println("Merging indexes into " + indexPath);
            try {
                for (FSDirectory tmp : directory_list) {
                    mainWriter.addIndexes(tmp);
                }
            } catch (IOException e) {
                System.out.println("Caught a " + e.getClass() + "with message: " + e.getMessage());
            }
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

            if (partialIndex)
                indexMulti(writer);
            else
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
