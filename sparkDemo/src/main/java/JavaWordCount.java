import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


/**
 * Created by Andy on 2018/3/26.
 */
public class JavaWordCount {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);


        JavaRDD<String> lines = jsc.textFile(args[0]);
        JavaPairRDD<String, Integer> tupleRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<String, Integer>(s, 1));

        JavaPairRDD<String, Integer> count = tupleRdd.reduceByKey((x, y) -> x + y);

        JavaPairRDD<Integer, String> swaped = count.mapToPair(tp -> tp.swap());

        JavaPairRDD<Integer, String> ordered = swaped.sortByKey(false);

        JavaPairRDD<String, Integer> result = ordered.mapToPair(tp -> tp.swap());

        result.saveAsTextFile(args[1]);



    }
}
