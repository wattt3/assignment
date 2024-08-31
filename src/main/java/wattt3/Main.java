package wattt3;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {
        final SparkConf sparkConf = new SparkConf().setAppName("assignment").setMaster("local[*]");

        final SparkSession sparkSession = SparkSession.builder()
            .config(sparkConf)
            .enableHiveSupport()
            .getOrCreate();
    }
}
