package es.udc.fi.ri.mipractica;

public class ManualRelevanceFeedbackNPL {
	
	static String index = "index";
	static int cut = 10;
	static int metrica = 0; //0 = P, 1 = R, 2 = MAP
	static String residual = "T";
	static String query = "T";
	static String indexingmodel = "tfidf";
	static float lambda = 0.5f;
    static float mu = 0.5f;
	
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
            	query = args[i + 1];
                i++;
            }
        	
        }
	}
	
    public static void main( String[] args )
    {
    	parseArguments(args);
    }
}
