package dbutil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DALTest {
    @Test
    public void getQueryForInsertIntoDailyTable() throws Exception {
        System.out.println(dal.getQueryForInsertIntoDailyTable(dal.finalTable,dal.dailyTable));
    }

    DAL dal = new DAL("misc.kandarp_test_daily");
    String testTableName;
    @Before
    public void setUp() throws Exception {
        testTableName = "misc.kandarp_test_process";
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getKeywordCLickedViewQuery() throws Exception {
        System.out.println(dal.getKeywordCLickedViewQuery("2017122500","2017122523",testTableName+"keyword_view"));
    }

    @Test
    public void getAdClickQuery() throws Exception {
        System.out.println(dal.getAdClickQuery("2017122500","2017122600",testTableName+"ad_click"));
    }

    @Test
    public void getQueryForViewIdFromAdClick() throws Exception {
        System.out.println(dal.getQueryForViewIdFromAdClick(testTableName+"ad_click",testTableName+"view_id"));
    }

    @Test
    public void getJoinedQuery() throws Exception {
        System.out.println(dal.getJoinedQuery(testTableName+"keyword_view",testTableName+"ad_click",testTableName+"view_id",testTableName+"l2a_table"));
    }

    @Test
    public void getQueryForCategoryIdToNameMap() throws Exception {
        System.out.println(dal.getQueryForCategoryIdToNameMap(testTableName+"id_to_name"));
    }

    @Test
    public void getQueryForCanonicalHashToCategoryMap() throws Exception {
        System.out.println(dal.getQueryForCanonicalHashToCategoryMap(testTableName+"url_to_category"));
    }

    @Test
    public void getQueryForFinalTable() throws Exception {
        System.out.println(dal.getQueryForFinalTable(testTableName+"l2a_table",testTableName+"url_to_category",testTableName+"id_to_name",testTableName+"final_table"));
    }

    @Test
    public void getQueryForDropTable() throws Exception {
        System.out.println(dal.getQueryForDropTable(testTableName));
    }

}