
import dbutil.DAL;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import util.SysProperties;

public class FFMDataAccumulator {


    public static void main(String cd[]){
        SparkConf conf = new SparkConf().setAppName(SysProperties.getInstance().getProperty("APP_NAME"));
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hsc = new HiveContext(sc);
        String endDate = "20180503";
        String startDate = cd[0];
//        String nominal_next = cd[2];
        System.out.println(endDate +" "+startDate+" ");
        new DAL("misc.ffm_v1_data_daily").addDailyData(endDate,startDate,hsc);
    }
}
