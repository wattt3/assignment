package wattt3;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Main {

    // %1$s - 연도 (e.g. 2019)
    // %2$s - 월 (e.g. Oct)
    private static final String PATH_TO_FILE_FORMAT = "archive/%1$s-%2$s.csv";
    private static final String PATH_TO_PARQUET_OUTPUT = "output";

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkInstance.INSTANCE.getSparkSession();

        final Dataset<Row> df = sparkSession.read()
            .option("header", true)
            .schema(getFileSchema())
            .csv(String.format(PATH_TO_FILE_FORMAT, "2019", "Oct"));

        final Dataset<Row> dfWithEventDate = df.withColumn("event_date",
            date_format(col("event_time"), "yyyy-MM-dd"));

        dfWithEventDate.write()
            .partitionBy("event_date")
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(PATH_TO_PARQUET_OUTPUT);
    }

    private static StructType getFileSchema() {
        return new StructType()
            .add("event_time", DataTypes.StringType)
            .add("event_type", DataTypes.StringType)
            .add("product_id", DataTypes.LongType)
            .add("category_id", DataTypes.LongType)
            .add("category_code", DataTypes.StringType)
            .add("brand", DataTypes.StringType)
            .add("price", DataTypes.DoubleType)
            .add("user_id", DataTypes.LongType)
            .add("user_session", DataTypes.StringType);
    }
}
