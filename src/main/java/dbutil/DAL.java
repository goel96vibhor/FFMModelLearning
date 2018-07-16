package dbutil;

import org.apache.spark.sql.hive.HiveContext;
import util.SysProperties;

public class DAL {

    private String keywordClickedViewTable;
    private String adClickTable;
    private String l2aTable;
    private String viewIdTable;
    private String categoryIdToNameMap;
    private String canonicalHashToUrlCategoryMap;
    public String finalTable;
    public String dailyTable;
    public String urlPerformanceTable;
    public String keywordPerformanceTable;
    public String finalFeatureTableWithGlobalStats;
    public String chunkMappingTable;
    public String nonPartitionedTable;
    public DAL(String dailyTable){
        SysProperties properties = SysProperties.getInstance();
        String version = properties.getProperty("VERSION");
        keywordClickedViewTable = "misc.ffm_v1_keyword_clicked_view_"+version;
        adClickTable = "misc.ffm_v1_ad_click_"+version;
        l2aTable = "misc.ffm_v1_l2a_keyword_clicked_view_"+version;
        viewIdTable = "misc.ffm_v1_viewId_"+version;
        categoryIdToNameMap = "misc.ffm_v1_kmean_url_category_"+version;
        canonicalHashToUrlCategoryMap = "misc.ffm_v1_canonical_hash_url_mapping_"+version;
        finalTable = "misc.ffm_v1_l2a_keyword_clicked_data_"+version;
        urlPerformanceTable = "misc.ffm_v1_url_performance_data_"+version;
        keywordPerformanceTable = "misc.ffm_v1_keyword_performance_data_"+version;
        finalFeatureTableWithGlobalStats = "misc.ffm_v1_final_feature_table_"+version;
        chunkMappingTable = "misc.ffm_v1_chunk_mapping_"+version;
        nonPartitionedTable  = "misc.ffm_v1_data_daily_partitioned";
        this.dailyTable = dailyTable;
    }

    public void addDailyData(String endDate, String startDate, HiveContext hsc){
//        hsc.sql(getQueryForDropTable(keywordClickedViewTable));
//        hsc.sql(getKeywordCLickedViewQuery(startDate+"00",endDate+"23",keywordClickedViewTable));
//        hsc.sql(getQueryForDropTable(adClickTable));
//        //System.out.println(getAdClickQuery(statsDate+"00",adClickSuccessFlag,adClickTable));
//        hsc.sql(getAdClickQuery(startDate+"00",endDate+"23",adClickTable));
//        hsc.sql(getQueryForDropTable(viewIdTable));
//        hsc.sql(getQueryForViewIdFromAdClick(adClickTable,viewIdTable));
//        hsc.sql(getQueryForDropTable(l2aTable));
//        hsc.sql(getJoinedQuery(keywordClickedViewTable,adClickTable,viewIdTable,l2aTable));
//        hsc.sql(getQueryForDropTable(categoryIdToNameMap));
//        hsc.sql(getQueryForCategoryIdToNameMap(categoryIdToNameMap));
//        hsc.sql(getQueryForDropTable(canonicalHashToUrlCategoryMap));
//        hsc.sql(getQueryForCanonicalHashToCategoryMap(canonicalHashToUrlCategoryMap));
//        hsc.sql(getQueryForDropTable(finalTable));
//        hsc.sql(getQueryForFinalTable(l2aTable,canonicalHashToUrlCategoryMap,categoryIdToNameMap,finalTable));

//        hsc.sql(getQueryForDropTable(urlPerformanceTable));
//        hsc.sql(getUrlPerformanceQuery(startDate, endDate, urlPerformanceTable));

//        hsc.sql(getQueryForDropTable(keywordPerformanceTable));
//        hsc.sql(getKeywordPerformanceQuery(startDate, endDate, keywordPerformanceTable));

//        hsc.sql(getQueryForDropTable(finalFeatureTableWithGlobalStats));
//        hsc.sql(getQueryForFinalFeatureWithGlobalStats(finalTable, finalFeatureTableWithGlobalStats));

//        hsc.sql(getQueryForDropTable(chunkMappingTable));
//        hsc.sql(getQueryforChunkTable(chunkMappingTable));

        hsc.sql("SET hive.exec.dynamic.partition = true" );
        hsc.sql("SET hive.exec.dynamic.partition.mode = nonstrict");
        hsc.sql(getQueryForInsertIntoDailyTable(nonPartitionedTable,dailyTable));
    }
    public String getKeywordCLickedViewQuery(String startTs , String endTs , String tableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("create table "+tableName+" as ");
        queryBuilder.append("select a.creative_id,a.country_code,a.metro_code,a.landing_page_framework_page_id,a.kid,a.kwt,a.kbc,a.domain_name,a.browser_id,a.device_id,a.os_id,a.url_category_id,a.kwd_category_id," +
                "a.view_id , a.hour_id , a.canonical_url_hash, a.trim_url, a.refering_domain as referer , a.kwp ,a.visitor_id ,1 as keyword_impression , sum(a.iskeywordclicked) as kwd_click,a.page_test_5 as external_call_bit,a.global_bucket_id, substr(a.ts,0,8) as stats_date " +
                "from web_log.keyword_clicked_view as a " +
                "where a.ts between "+startTs+" and "+endTs+" "+
                "group by a.creative_id,a.country_code,a.metro_code,a.landing_page_framework_page_id,a.kid,a.kwt,a.kbc,a.domain_name,a.browser_id,a.device_id,a.os_id,a.url_category_id,a.kwd_category_id, " +
                "a.view_id , a.hour_id , a.canonical_url_hash, a.trim_url, a.refering_domain, a.kwp, a.visitor_id,a.page_test_5,a.global_bucket_id , substr(a.ts,0,8)");

        return queryBuilder.toString();
    }
    public String getAdClickQuery(String startTs , String endTs , String tableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("create table "+tableName+" as ");
        queryBuilder.append("select a.landing_page_view_id as view_id, a.keyword_id , sum(1) as ad_click " +
                "from web_log.ad_click as a " +
                "JOIN RELATIONAL_DB.gbl_view_click_status_lookup c on a.click_status = c.status " +
                "WHERE a.ts BETWEEN "+startTs+" AND "+endTs+" "+
                "AND (COALESCE(a.audit_spam_flag,false) = false ) " +
                "AND c.is_spam in (0,2) AND c.type in ('Click','click') " +
                "group by a.landing_page_view_id , a.keyword_id");

        return queryBuilder.toString();
    }
    public String getQueryForViewIdFromAdClick(String adClickTableName , String resultTableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("create table "+resultTableName+" as ");
        queryBuilder.append("select distinct view_id from "+adClickTableName);
        return queryBuilder.toString();
    }
    public String getJoinedQuery(String keywordClickedViewTableName,String adClickTableName,String viewIdTable,String resultTableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("create table "+resultTableName+" as ");
        queryBuilder.append("select a.creative_id,a.country_code,a.metro_code,a.landing_page_framework_page_id,a.kid,a.kwt,a.kbc,a.domain_name,a.browser_id,a.device_id,a.os_id,a.url_category_id,a.kwd_category_id, " +
                "a.view_id , a.hour_id , a.canonical_url_hash, a.trim_url, a.referer , a.kwp, a.visitor_id , sum(a.keyword_impression) as keyword_impression , sum(a.kwd_click) as kwd_click , sum(coalesce(b.ad_click,0)) as ad_click ,a.external_call_bit,a.global_bucket_id ,a.stats_date " +
                "from "+keywordClickedViewTableName+" as a " +
                "left join "+adClickTableName+"  as b on a.view_id = b.view_id and a.kid = b.keyword_id " +
                "join "+viewIdTable +" as c on coalesce(a.view_id,'NULL') = coalesce(c.view_id,'NULL') " +
                "group by a.creative_id,a.country_code,a.metro_code,a.landing_page_framework_page_id,a.kid,a.kwt,a.kbc,a.domain_name,a.browser_id,a.device_id,a.os_id,a.url_category_id,a.kwd_category_id, " +
                "a.view_id , a.hour_id , a.canonical_url_hash, a.trim_url, a.referer ,a.external_call_bit,a.global_bucket_id, a.kwp, a.visitor_id, a.stats_date ");
        return queryBuilder.toString();
    }
    public String getQueryForCategoryIdToNameMap(String resultTableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("create table "+resultTableName+" as ");
        queryBuilder.append("select distinct a.category_id , a.category_name " +
                "from relational_db.kmean_category_master as a");
        return queryBuilder.toString();
    }
    public String getQueryForCanonicalHashToCategoryMap(String resultTableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("create table "+resultTableName+" as ");
        queryBuilder.append("select distinct a.chash as canonical_url_hash , a.cid1 as url_category_id from relational_db.kmean_url_category_mapping_hbase as a");
        return queryBuilder.toString();
    }
    public String getQueryForFinalTable(String l2aTable,String canonicalHashToUrlMapping,String categoryToNameMapping,String resultTableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("create table "+resultTableName+" as ");
        queryBuilder.append("select a.creative_id,a.country_code,a.metro_code,c.keyword_count,a.kid,a.kwt,a.kbc,a.domain_name,a.browser_id,a.device_id,a.os_id,f.category_name as url_category,a.kwd_category_id," +
                "view_id, a.hour_id , a.canonical_url_hash ,a.trim_url, a.referer as http_referer , a.kwp ,a.external_call_bit,a.global_bucket_id,avg(1.0/(1 + coalesce(g.score,0))) as weight, " +
                "sum(a.keyword_impression) as keyword_impression , sum(a.kwd_click) as kwd_click , sum(a.ad_click) as ad_click , a.stats_date " +
                "from "+ l2aTable+" as a " +
                "LEFT JOIN relational_db.GBL_HTMLFRAMEWORK_PAGE c on (cast(a.landing_page_framework_page_id as bigint) = c.page_id) " +
                "LEFT JOIN "+ canonicalHashToUrlMapping +" as d on a.canonical_url_hash = d.canonical_url_hash " +
                "LEFT JOIN "+ categoryToNameMapping +" as f on cast(coalesce(d.url_category_id,a.url_category_id) as bigint) = f.category_id " +
                "LEFT JOIN LEARNING.TEMPLATE_TYPE_POSITION_BIAS  as g ON c.page_type_id = g.page_type_id AND c.template_size_id = g.template_size_id " +
                "AND c.keyword_count = g.keyword_count AND c.template_position = g.template_position AND a.kwp = g.keyword_position_id " +
                "group by a.creative_id,a.country_code,a.metro_code,c.keyword_count,a.kid,a.kwt,a.kbc,a.domain_name,a.browser_id,a.device_id,a.os_id,f.category_name,a.kwd_category_id, " +
                "view_id , a.hour_id , a.canonical_url_hash ,a.trim_url , a.referer , a.kwp,a.external_call_bit , a.global_bucket_id , a.stats_date");
        return queryBuilder.toString();
    }

    public String getQueryForInsertIntoDailyTable(String inputTableName , String dailyTable){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("insert overwrite table "+dailyTable+" PARTITION (chunk_no) " +
                "select creative_id , " +
                "  country_code , " +
                "  metro_code , " +
                "  keyword_count , " +
                "  kid , " +
                "  kwt , " +
                "  kbc , " +
                "  domain_name , " +
                "  browser_id , " +
                "  device_id , " +
                "  os_id , " +
                "  url_category , " +
                "  kwd_category_id , " +
                "  view_id , " +
                "  hour_id , " +
                "  canonical_url_hash , " +
                "  trim_url , " +
                "  http_referer , " +
                "  kwp , " +
                "  weight , " +
                "  keyword_impression , " +
                "  kwd_click , " +
                "  ad_click , " +
                "  url_tot_imp , url_tot_conv , url_total_revenue, kwd_tot_imp, kwd_tot_conv , kwd_total_revenue,"+
                "  global_bucket_id , " +
                "  external_call_bit , " +
                "  stats_date , " +
                "  chunk_no  " +
                "from "+inputTableName);
        return queryBuilder.toString();
    }

    public String getUrlPerformanceQuery(String startTs , String endTs , String tableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("create table "+tableName+" as ");
        queryBuilder.append("select a.canonical_hash, sum(a.scaled_impression) as impressions, sum(a.conversion) as conversions, sum(a.revenue) as revenue" +
                "from taxonomy.keyword_hash_daily as a " +
                "WHERE a.date BETWEEN "+startTs+" AND "+endTs+" "+
                "group by a.canonical_hash ");

        return queryBuilder.toString();
    }

    public String getKeywordPerformanceQuery(String startTs , String endTs , String tableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("create table "+tableName+" as ");
        queryBuilder.append("select a.keyword_id, sum(a.scaled_impression) as impressions, sum(a.conversion) as conversions, sum(a.revenue) as revenue" +
                "from taxonomy.keyword_hash_daily as a " +
                "WHERE a.date BETWEEN "+startTs+" AND "+endTs+" "+
                "group by a.keyword_id ");

        return queryBuilder.toString();
    }

    public String getQueryForFinalFeatureWithGlobalStats(String finalTable,String resultTableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("create table "+resultTableName+" as ");
        queryBuilder.append("select a.creative_id,a.country_code,a.metro_code,a.keyword_count,a.kid,a.kwt,a.kbc,a.domain_name,a.browser_id,a.device_id,a.os_id,a.url_category,a.kwd_category_id, " +
                "a.view_id , a.hour_id , a.canonical_url_hash ,a.trim_url , a.http_referer , a.kwp,a.external_call_bit , a.global_bucket_id , a.weight , " +
                " sum(a.keyword_impression) as keyword_impression , sum(a.kwd_click) as kwd_click , sum(a.ad_click) as ad_click , " +
                " b.impressions as url_tot_imp, b.conversions as url_tot_conv, b.revenue as url_total_revenue, c.impressions as kwd_tot_imp, c.conversions as kwd_tot_conv, c.revenue as kwd_total_revenue, a.stats_date " +
                "from "+ finalTable+" as a " +
                "LEFT JOIN " + urlPerformanceTable + " as b on a.canonical_url_hash = b.canonical_hash" +
                "LEFT JOIN " + keywordPerformanceTable + " as c on a.kid = c.keyword_id" +
                "group by a.creative_id,a.country_code,a.metro_code,c.keyword_count,a.kid,a.kwt,a.kbc,a.domain_name,a.browser_id,a.device_id,a.os_id,a.url_category,a.kwd_category_id, " +
                "a.view_id , a.hour_id , a.canonical_url_hash ,a.trim_url , a.http_referer , a.kwp,a.external_call_bit , a.global_bucket_id , a.weight , b.impressions , b.conversions , b.revenue , c.impressions , c.conversions , c.revenue ,a.stats_date");
        return queryBuilder.toString();
    }

    public String getQueryforChunkTable(String tableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("create table "+tableName+" as ");
        queryBuilder.append("select a.canonical_url_hash, count(1) count, CEIL (CAST(50 AS INT)*(SUM(count(1)) OVER (ORDER BY domain_name))/(SUM(count(1)) OVER())) chunk_no" +
                "from "+finalFeatureTableWithGlobalStats+ " as a " +
                "group by a.domain_name");

        return queryBuilder.toString();
    }

    public String getQueryForDropTable(String tableName){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("drop table if exists "+tableName);
        return queryBuilder.toString();
    }
}
