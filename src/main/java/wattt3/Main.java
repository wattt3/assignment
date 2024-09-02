package wattt3;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.io.File;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Main {

    // %1$s - 연도 (e.g. 2019)
    // %2$s - 월 (e.g. Oct)
    private static final String PATH_TO_FILE_FORMAT = "archive/%1$s-%2$s.csv";
    private static final String ABSOLUTE_PATH_TO_OUTPUT = "output";
    private static final String HIVE_EXTERNAL_TABLE = "ASSIGNMENT";
    // %1$s - Absolute path to parquet output
    // %2$s - HIVE EXTERNAL TABLE 이름
    private static final String CREATE_EXTERNAL_TABLE = """
        CREATE EXTERNAL TABLE IF NOT EXISTS %1$s (
          EVENT_TIME STRING,
          EVENT_TYPE STRING,
          PRODUCT_ID BIGINT,
          CATEGORY_ID BIGINT,
          CATEGORY_CODE STRING,
          BRAND STRING,
          PRICE DOUBLE,
          USER_ID BIGINT,
          USER_SESSION STRING
        ) PARTITIONED BY (EVENT_DATE TIMESTAMP)
        STORED AS PARQUET LOCATION '%2$s'
        TBLPROPERTIES ("parquet.compression"="SNAPPY");
        """;
    // %s - HIVE EXTERNAL TABLE 이름
    private static final String METASTORE_CHECK_QUERY_FORMAT = "MSCK REPAIR TABLE %s";

    public static void main(String[] args) {
        System.out.println("시작 연도와 월, 끝 연도와 월을 입력하세요. (e.g. 2019 10 2019 11)");
        final DateRange dateRange = inputDateRange();

        final SparkSession sparkSession = SparkInstance.INSTANCE.getSparkSession();

        final Dataset<Row> df = readCsvLogs(sparkSession, dateRange);

        final Dataset<Row> dfWithEventDate = df.withColumn("event_date",
            date_format(col("event_time"), "yyyy-MM-dd"));

        final Dataset<Row> dfWithCheckpoint = dfWithEventDate.checkpoint();

        dfWithCheckpoint.write()
            .partitionBy("event_date")
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(ABSOLUTE_PATH_TO_OUTPUT);

        final String createExternalTableQuery = String.format(CREATE_EXTERNAL_TABLE,
            HIVE_EXTERNAL_TABLE, ABSOLUTE_PATH_TO_OUTPUT);
        final String metastoreCheckQuery = String.format(METASTORE_CHECK_QUERY_FORMAT,
            HIVE_EXTERNAL_TABLE);

        sparkSession.sql(createExternalTableQuery);
        sparkSession.sql(metastoreCheckQuery);
    }

    private static DateRange inputDateRange() {
        try (final Scanner scanner = new Scanner(System.in)) {
            final int startYear = scanner.nextInt();
            final int startMonth = scanner.nextInt();
            final int endYear = scanner.nextInt();
            final int endMonth = scanner.nextInt();

            return new DateRange(startYear, startMonth, endYear, endMonth);
        }
    }

    private static Dataset<Row> readCsvLogs(SparkSession sparkSession, DateRange dateRange) {
        YearMonth start = YearMonth.of(dateRange.startYear(), dateRange.startMonth());
        YearMonth end = YearMonth.of(dateRange.endYear(), dateRange.endMonth());

        List<Dataset<Row>> logs = new ArrayList<>();

        while (!start.isAfter(end)) {
            final String year = String.valueOf(start.getYear());
            final String month = numberToMonth(start.getMonthValue());
            final String pathToFile = String.format(PATH_TO_FILE_FORMAT, year, month);

            File file = new File(pathToFile);
            if (!file.exists()) {
                System.out.printf("해당 경로에 파일이 존재하지 않음: %s\n", pathToFile);
                start = start.plusMonths(1);
                continue;
            }

            Dataset<Row> df = sparkSession.read()
                .option("header", true)
                .schema(logSchema())
                .csv(pathToFile);

            logs.add(df);

            start = start.plusMonths(1);
        }

        return logs.stream().reduce(Dataset::union).orElse(sparkSession.emptyDataFrame());
    }

    private static String numberToMonth(int month) {
        return switch (month) {
            case 1 -> "Jan";
            case 2 -> "Feb";
            case 3 -> "Mar";
            case 4 -> "Apr";
            case 5 -> "May";
            case 6 -> "Jun";
            case 7 -> "Jul";
            case 8 -> "Aug";
            case 9 -> "Sep";
            case 10 -> "Oct";
            case 11 -> "Nov";
            case 12 -> "Dec";
            default -> throw new IllegalArgumentException("Invalid month number: " + month);
        };
    }

    private static StructType logSchema() {
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

    private record DateRange(int startYear, int startMonth, int endYear, int endMonth) {

    }
}
