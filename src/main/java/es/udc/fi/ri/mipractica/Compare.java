package es.udc.fi.ri.mipractica;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.inference.WilcoxonSignedRankTest;
import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.apache.commons.math3.stat.ranking.TiesStrategy;
import java.lang.Double;

public class Compare {
	
	static String results1 = "results1";
	static String results2 = "results2";
	static String test = "t";
	static double alpha = 0.0;
	
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
	
	private static double[] getArray(Scanner scanner) {
		List<Double> lst = new ArrayList<Double>();
		while (scanner.hasNextLine()) {
			String[] line = scanner.nextLine().split(" ");
			double sample = Double.parseDouble(line[line.length-1]);
			lst.add(sample);
		}
		double[] samples=new double[lst.size()];
		int index = 0;
		for(final Double value:lst)
			samples[index++]=value;
		return samples;
	}
	
	private static void doTTest(Scanner sc1, Scanner sc2) {
		double[] samples1=getArray(sc1);
		double[] samples2=getArray(sc2);
		TTest tTest = new TTest();
		double pvalue = tTest.pairedTTest(samples1,samples2);
		boolean result = tTest.pairedTTest(samples1,samples2, alpha);
		String[] div = String.valueOf(pvalue).split("\\.");
		System.out.println("Results for t-test");
		System.out.println("Result\tP-value");
		System.out.print("------\t");
		int max = Math.max(div[0].length()+div[1].length()+1,7);
		for(int i = 0; i< max;i++)
			System.out.print("-");
		System.out.println();
		System.out.println(result+"\t"+pvalue);
	}
	
	private static void doWTest(Scanner sc1, Scanner sc2) {
		double[] samples1=getArray(sc1);
		double[] samples2=getArray(sc2);
		WilcoxonSignedRankTest wTest = new WilcoxonSignedRankTest(NaNStrategy.FIXED,TiesStrategy.AVERAGE);
		double pvalue = wTest.wilcoxonSignedRankTest(samples1,samples2,false);
		String[] div = String.valueOf(pvalue).split("\\.");
		System.out.println("Results for Wilcoxon signed-rank test");
		System.out.println("P-value");
		int max = Math.max(div[0].length()+div[1].length()+1,7);
		for(int i = 0; i< max;i++)
			System.out.print("-");
		System.out.println();
		System.out.println(pvalue);
	}
	
    public static void main( String[] args )
    {
    	parseArguments(args);
    	
    	Scanner reader1 = null;
    	Scanner reader2 = null;
    	
    	try {
    		File file1 = new File(results1);
    		reader1 = new Scanner(file1);
    	}catch(FileNotFoundException e) {
    		System.err.println("File results1 does not exist");
            System.exit(1);
    	}
    	
    	try {
    		File file2 = new File(results2);
    		reader2 = new Scanner(file2);
    	}catch(FileNotFoundException e) {
    		System.err.println("File results2 does not exist");
            System.exit(1);
    	}
    	
    	switch(test) {
    	case "t": doTTest(reader1, reader2);
    			 break;
    	case "wilcoxon": doWTest(reader1,reader2);
    			 break;
    	default: System.err.println("Error in test. Test not recognized");
        		 System.exit(1);
    	}
    	
    }
}
