package spark.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.concurrent.*;

public class SparkJobConcurrent {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().getOrCreate();

        String[] s = {""};
        int len = s.length;

        ArrayList<Future<Dataset<Row>>> arrayList = new ArrayList();

        //创建线程池
        ExecutorService executorService = Executors.newFixedThreadPool(30);

        for (int i = 0; i < len; i++) {
            String oem = s[i];

            //the task is submitted as a lambda
            Callable<Dataset<Row>> call = () -> spark.emptyDataFrame();

            Future<Dataset<Row>> future = executorService.submit(call);

            arrayList.add(future);
        }

        executorService.shutdown();

        Dataset<Row> ds = spark.emptyDataFrame();
        try{
            if (executorService.awaitTermination(10L, TimeUnit.MINUTES)) {
                for (int i = 0; i < len; i++) {
                    if (i == 0) {
                        ds = arrayList.get(i).get();
                    } else {
                        ds = ds.union(arrayList.get(i).get());
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        ds.repartition(1)
            .write()
            .mode("append")
            .partitionBy("data_type")
            .format("parquet")
            .saveAsTable("dwb.third_party_url");

        spark.close();
    }
}