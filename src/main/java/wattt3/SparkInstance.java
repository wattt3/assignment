package wattt3;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public enum SparkInstance {
    INSTANCE;

    private final SparkSession sparkSession;

    SparkInstance() {
        final SparkConf sparkConf = new SparkConf()
            .setAppName("assignment")
            .set("spark.sql.session.timeZone", "Asia/Seoul")
            .setMaster("local[*]");

        sparkSession = SparkSession.builder()
            .config(sparkConf)
            .enableHiveSupport()
            .getOrCreate();
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }
}
