package spark.batch

import org.apache.spark.sql.functions.{explode, regexp_replace, split, substring_index}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SparkSQLRow2Column {

    //Json数组行转列
    def explodeFunc(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
        import spark.implicits._
        df.select($"user_id",
            explode(
                split(
                    substring_index(
                        substring_index(
                            regexp_replace($"contacts", "},", "}^*^*"),
                            "[", -1),
                        "]", 1),
                    "\\^\\*\\^\\*"
                )
            ).as("contact")
        )
    }
}
