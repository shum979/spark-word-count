package spark;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public final class JavaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);


        List<String> output = lines.collect();
        for (String tuple : output) {
            System.out.println(tuple + ": ");
        }
        ctx.stop();
    }
}
