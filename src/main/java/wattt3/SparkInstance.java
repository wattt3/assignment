package wattt3;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public enum SparkInstance {
    INSTANCE;

    private static final String PATH_TO_CHECKPOINT = "checkpoint";

    private final SparkSession sparkSession;

    SparkInstance() {
        final SparkConf sparkConf = new SparkConf()
            .setAppName("assignment")
            .set("spark.sql.session.timeZone", "Asia/Seoul")
            .set("spark.task.maxFailures", "3")
            .setMaster("local[*]");

        sparkSession = SparkSession.builder()
            .config(sparkConf)
            .enableHiveSupport()
            .getOrCreate();

        sparkSession.sparkContext().setCheckpointDir(PATH_TO_CHECKPOINT);
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }
}
