package wattt3;

import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkInstance.INSTANCE.getSparkSession();
    }
}
