package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        System.out.println("Test spark application started .......");
        System.out.println("SPark on openShift started .......");

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        System.out.println("Program argument received ... "+ args[0]);

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        System.out.println("Java spark context started .....");

        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        System.out.println("spark read file completed .....");

        List<String> output = lines.collect();
        for (String tuple : output) {
            System.out.println(tuple + ": ");
        }

        System.out.println("Files lines collected and printed .....");
        ctx.stop();
    }
}
