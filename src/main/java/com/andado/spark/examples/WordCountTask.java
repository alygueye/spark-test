package com.andado.spark.examples;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * WordCountTask class, we will call this class with the test WordCountTest.
 */
public class WordCountTask {


    private static final Pattern SPACE = Pattern.compile(" ");

    /**
     * We use a logger to print the output. Sl4j is a common library which works with log4j, the
     * logging system used by Apache Spark.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTask.class);

    /**
     * This is the entry point function when the task is called with spark-submit.sh from command
     * line. In our example we will call the task from a WordCountTest instead. See {@see
     * http://spark.apache.org/docs/latest/submitting-applications.html}
     */
    public static void main(String[] args) {
        checkArgument(args.length > 1, "Please provide the path of input file as first parameter.");
        new WordCountTask().run(args[1]);
    }

    /**
     * The task body
     */
    public void run(String inputFilePath) {
    /*
     * This is the address of the Spark cluster. We will call the task from WordCountTest and we
     * use a local standalone cluster. [*] means use all the cores available.
     * See {@see http://spark.apache.org/docs/latest/submitting-applications.html#master-urls}.
     */
        String master = "spark://MacBook-Pro-de-aly-2.local:7077";

    /*
     * Initialises a Spark context.
     */
        SparkSession spark = SparkSession
                .builder()
                .appName(WordCountTask.class.getName())
                .master(master)
                .getOrCreate();
    /*
     * Performs a work count sequence of tasks and prints the output with a logger.
     */
        spark.read().textFile(inputFilePath)
                .javaRDD()
                .flatMap(text -> Arrays.asList(text.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .foreach(result -> LOGGER.info(
                        String.format("Word [%s] count [%d].", result._1(), result._2)));
    }
}
