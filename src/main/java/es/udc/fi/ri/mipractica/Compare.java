package es.udc.fi.ri.mipractica;

public class Compare {
	
	static String results1 = "results1";
	static String results2 = "results2";
	static String test = "t";
	static float alpha = 0.0f;
	
	private static void parseArguments(String[] args) {

        String usage = "java -jar Compare-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-results results1 results2] [-test <t | wilcoxon> alpha]";

        if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
            System.out.println(usage);
            System.exit(0);
        }

        for (int i = 0; i < args.length; i++) {
            if ("-results".equals(args[i])) {
            	if(!args[i+1].startsWith("-") && !args[i+2].startsWith("-")) {
            		results1=args[i+1];
                	results2=args[i+2];
                	i=i+2;
            	}else {
            		System.err.println("Error in results. Must provide 2 results files");
                    System.exit(1);
            	}
            }else if ("-test".equals(args[i])) {
            	if(!args[i+1].startsWith("-") && !args[i+2].startsWith("-")) {
            		test=args[i+1];
            		try {
            			alpha=Float.parseFloat(args[i+2]);
            		}catch (NumberFormatException e) {
            			System.err.println("Error in alpha. Must provide correct alpha value");
                        System.exit(1);
            		}
                	i=i+2;
            	}else {
            		System.err.println("Error in test. Must provide correct test method");
                    System.exit(1);
            	}
            }
        }
    }
	
    public static void main( String[] args )
    {
    	parseArguments(args);
    }
}
