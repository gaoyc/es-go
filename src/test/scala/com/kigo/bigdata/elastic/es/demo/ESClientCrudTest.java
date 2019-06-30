package com.kigo.bigdata.elastic.es.demo;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * <p>Es客户端API方式CRUD操作示例<p/>
 * <p>  以下环境测试通过: <p/>
 * <p>     Es集群版本: 5.6.0 <p/>
 * Created by kigo on 18-1-16.
 */
public class ESClientCrudTest {

    Log log = LogFactory.getLog(ESClientCrudTest.class);

    private TransportClient client;
    private IndexRequest source;

    private String clusterName = "elasticsearch";
    //private String hostName = "localhost";
    private String hostName = "127.0.0.1";
    private  int port = 9300;

    private String testIndext = "index_test";
    private String testType = "type_test";
    private String testId = "1";



    /**
     * 获取连接
     * @throws Exception
     */
    @Before
    public void before() throws Exception {

        // 通过setting对象指定集群配置信息, 配置的集群名
        Settings settings = Settings.builder().put("cluster.name", clusterName) // 设置集群名
                .put("client.transport.ignore_cluster_name", true) // 忽略集群名字验证, 打开后集群名字不对也能连接上
                .build();

        // 旧版本API
        //client = TransportClient.builder().settings(settings).build()
        //      .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("192.168.50.37", 9300)));

        //5.x.x 版本API
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), port))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), port));

        System.out.println("success connect");
    }

    /**
     * 查看集群信息
     */
    // @Test
    public void testConnToCluster() {
        List<DiscoveryNode> nodes = client.connectedNodes();
        for (DiscoveryNode node : nodes) {
            System.out.println(node.getHostAddress());
        }
    }


    /**
     * 进行连接测试
     * @throws Exception
     */
    // @Test
    public void testCreateIndex() throws Exception {
        XContentBuilder source = createJson4();
        // 存json入索引中
        IndexResponse response = client.prepareIndex(testIndext, testType, testId).setSource(source).get();
//        // 结果获取
        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        DocWriteResponse.Result result = response.getResult();

        System.out.println(index + " : " + type + ": " + id + ": " + version + ": " + result);
    }

    /**
     * get API 获取指定文档信息
     */
    // @Test
    public void testQueryByGet() {
//        GetResponse response = client.prepareGet(testIndext, testType, testId)
//                                .get();
        GetResponse response = client.prepareGet(testIndext, testType, testId)
                .setOperationThreaded(false)    // 线程安全
                .get();
        System.out.println(response.getSourceAsString());
    }


    // @Test
    public void testQueryByMatch() {
        matchQuery(testIndext, "user", "kigo");
    }


    /**
     * 测试更新 update API
     * 使用 updateRequest 对象
     * @throws Exception
     */
    //@Test
    public void testUpdate() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(testIndext);
        updateRequest.type(testType);
        updateRequest.id(testId);
        updateRequest.doc(XContentFactory.jsonBuilder()
                .startObject()
                // 对没有的字段添加, 对已有的字段替换
                .field("gender", "male")
                .field("message", "hello")
                .endObject());
        UpdateResponse response = client.update(updateRequest).get();

        // 打印
        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        System.out.println(index + " : " + type + ": " + id + ": " + version);
    }


    /**
     * 测试update
     * 使用updateRequest
     * @throws Exception
     * @throws InterruptedException
     */
    // //@Test
    public void testUpdateByScript() throws InterruptedException, Exception {
        UpdateRequest updateRequest = new UpdateRequest(testIndext, testType, testId)
                .script(new Script("ctx._source.gender=\"male\""));
        UpdateResponse response = client.update(updateRequest).get();
    }


    /**
     * 测试 delete api
     */
    //@Test
    public void testDelete() {
        DeleteResponse response = client.prepareDelete(testIndext, testType, testId)
                .get();
        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        System.out.println(index + " : " + type + ": " + id + ": " + version);
    }



    /**
     * 使用es的帮助类生成测试doc
     */
    private XContentBuilder createJson4() throws Exception {
        // 创建json对象, 其中一个创建json的方式
        XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "Kigo")
                .field("postDate", new Date())
                .field("message", "trying to demo the crud op of the ElasticSearch")
                .field("class", new Random().nextInt(5))
                .field("stores", new Random().nextInt(100))
                .endObject();
        return source;
    }

    public  void matchQuery(String indices,String field,String queryString){
        SearchResponse searchResponse = client.prepareSearch(indices)
                .setQuery(QueryBuilders.matchQuery(field, queryString))
                .execute()
                .actionGet();
        SearchHits searchHits = searchResponse.getHits();
        System.out.println("---------------matchquery--在["+field+"]中搜索关键字["+queryString+"]---------------------");
        System.out.println("共匹配到:"+searchHits.getTotalHits()+"条记录!");
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            Map<String, Object> sourceAsMap = searchHit.sourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                System.out.println(string+":"+sourceAsMap.get(string));
            }
            System.out.println();
        }
    }

    /**
     * 使用bulk processor
     * @throws Exception
     */
    //@Test
    public void testBulkToEs() throws Exception {
        // 创建BulkPorcessor对象
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new Listener() {
            public void beforeBulk(long paramLong, BulkRequest paramBulkRequest) {
                // TODO Auto-generated method stub
            }

            // 执行出错时执行
            public void afterBulk(long paramLong, BulkRequest paramBulkRequest, Throwable paramThrowable) {
                // TODO Auto-generated method stub
                log.error(paramThrowable);
                System.err.println("err occurs when bulk load to es");
            }

            public void afterBulk(long paramLong, BulkRequest paramBulkRequest, BulkResponse paramBulkResponse) {
                // TODO Auto-generated method stub
                System.out.println("finish the bulk load to es, bulk size="+paramBulkRequest.numberOfActions());
            }
        })
                // n次请求执行一次bulk
                .setBulkActions(100)
                // 1gb的数据刷新一次bulk
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
                // 固定5s必须刷新一次
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                // 并发请求数量, 0不并发, 1并发允许执行
                .setConcurrentRequests(1)
                // 设置退避, 100ms后执行, 最大请求3次
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        for (int i = 0; i < 1000; i++) {
            bulkProcessor.add(new IndexRequest(testIndext, testType, "blk_"+i).source(createJson4()));
        }

        // 关闭
        bulkProcessor.awaitClose(30, TimeUnit.SECONDS);
        // 或者
        //bulkProcessor.close();
    }

    //@Test
    public void testQueryByPageSize() {
        int from = 0;
        int pageSize = 10; //单页大小
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("message", "demo","crud");
        SearchRequestBuilder builder = client.prepareSearch(testIndext)
                .setQuery(termsQueryBuilder);
                //.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

        long count = builder.execute().actionGet().getHits().getTotalHits();
        int page = (int)Math.ceil(count/pageSize);
        for (int i = 0; i < page; i++) {
            HighlightBuilder highlightBuilder = new HighlightBuilder().field("message").requireFieldMatch(false);
            highlightBuilder.preTags("<span style=\"color:red\">"); //"<em>"
            highlightBuilder.postTags("</span>");
            highlightBuilder.numOfFragments(250); //设置高亮内容长度
            builder = builder
                    // 设置查询类型
                    // 1.SearchType.DFS_QUERY_THEN_FETCH = 精确查询
                    // 2.SearchType.SCAN = 扫描查询,无序
                    // 3.SearchType.COUNT = 默认值
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .highlighter(highlightBuilder);

            from = i * pageSize;
            // 设置查询数据的位置,分页用
            SearchResponse searchResponse = builder
                    .setFrom(from)
                    // 设置查询结果集的最大条数
                    .setSize(pageSize)
                    // 设置是否按查询匹配度排序
                    .setExplain(true)
                    //.addSort("_id", SortOrder.ASC) //排序
                    .execute()
                    .actionGet();
            SearchHits searchHits = searchResponse.getHits();
            System.out.println("total="+count+"\tpageSize="+pageSize+"\ttotalPage="+page+"\tcurPage="+i+"\tfrom="+from);
            SearchHit[] hits = searchHits.getHits();
            for (SearchHit searchHit : hits) {
                //获取高亮的字段
                Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
                HighlightField highlightField = highlightFields.get("message");
//                System.out.println("高亮字段:"+highlightField.getName()+"\n高亮部分内容:"+highlightField.getFragments()[0].string());
                Map<String, Object> sourceAsMap = searchHit.sourceAsMap();
                Set<String> keySet = sourceAsMap.keySet();
                for (String string : keySet) {
//                    System.out.println(string+":"+sourceAsMap.get(string));
                }
            }
        }

        System.out.println("total="+count+"\tpageSize="+pageSize+"\tpage="+page);

    }


    public void testQueryByScroll(){
        QueryBuilder qb = QueryBuilders.termsQuery("message", "demo","crud");
        // 按照price 降序  每次查询2条  第一次不需要设置sroll  scrollid
        SearchResponse scrollResp = client.prepareSearch(testIndext).setTypes(testType)
                .addSort("postDate", SortOrder.DESC)
                .setScroll(new TimeValue(30000))
                .setQuery(qb)
                .setSize(2).get();

        System.out.println("length: " + scrollResp.getHits().getHits().length);
        int count = 1;
        do{
            System.out.println("第 " +count+ " 次");
            System.out.println();
            for (SearchHit hit : scrollResp.getHits().getHits()){
                System.out.println(hit.getScore() + " --> " +hit.getSourceAsString());
            }


            System.out.println("scrollid:  "+scrollResp.getScrollId());

            //设置sroll id
            scrollResp =client.prepareSearchScroll(scrollResp.getScrollId()).
                    setScroll(new TimeValue(60000)).execute().actionGet();
            System.out.println();

            count++;

        } while (scrollResp.getHits().getHits().length !=0);


        client.close();

    }


    /**
     * match all查询，对查询结果排序，并高亮显示查询字段
     */
    @Test
    public  void testMatchAllQuery(){

        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchRequestBuilder sbuilder = client.prepareSearch(testIndext).setTypes(testType);

        HighlightBuilder highlightBuilder = new HighlightBuilder().field("message").requireFieldMatch(false);
        highlightBuilder.preTags("<span style=\"color:red\">"); //"<em>"
        highlightBuilder.postTags("</span>");
        highlightBuilder.numOfFragments(250); //设置高亮内容长度
        sbuilder = sbuilder
                .addFieldDataField("message")
                .addFieldDataField("stores")
                .addSort("stores", SortOrder.ASC) //排序 stores
                .highlighter(highlightBuilder);


        SearchResponse searchResponse = sbuilder.setQuery(qb).execute()
                .actionGet();

        SearchHits searchHits = searchResponse.getHits();
        System.out.println("共匹配到:"+searchHits.getTotalHits()+"条记录!");
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {

            //获取高亮的字段
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("message");
            System.out.println("高亮字段:"+highlightField.getName()+"\n高亮部分内容:"+highlightField.getFragments()[0].string());

            Map<String, Object> sourceAsMap = searchHit.sourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                    System.out.println(string+":"+sourceAsMap.get(string));
            }

        }
    }

    // @Test
    public void testQueryByHighlighter() {
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("message", "demo","crud");
        SearchRequestBuilder builder = client.prepareSearch(testIndext)
                .setQuery(termsQueryBuilder)
                // 设置查询类型
                // 1.SearchType.DFS_QUERY_THEN_FETCH = 精确查询
                // 2.SearchType.SCAN = 扫描查询,无序
                // 3.SearchType.COUNT = 不设置的话,这个为默认值,还有的自己去试试吧
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

        HighlightBuilder highlightBuilder = new HighlightBuilder().field("message").requireFieldMatch(false);
        highlightBuilder.preTags("<span style=\"color:red\">"); //"<em>"
        highlightBuilder.postTags("</span>");
        highlightBuilder.numOfFragments(250); //设置高亮内容长度
        builder = builder.highlighter(highlightBuilder);

        // 设置查询数据的位置,分页用
        SearchResponse searchResponse = builder
                .setFrom(0)
                // 设置查询结果集的最大条数
                .setSize(10)
                // 设置是否按查询匹配度排序
                .setExplain(true)
                .execute()
                .actionGet();
        SearchHits searchHits = searchResponse.getHits();
        System.out.println("-----------------termsQuery---------------------");
        System.out.println("共匹配到:"+searchHits.getTotalHits()+"条记录!");
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit : hits) {
            //获取高亮的字段
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("message");
            System.out.println("高亮字段:"+highlightField.getName()+"\n高亮部分内容:"+highlightField.getFragments()[0].string());
            Map<String, Object> sourceAsMap = searchHit.sourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String string : keySet) {
                System.out.println(string+":"+sourceAsMap.get(string));
            }
            System.out.println();
        }
    }


    /**
     * 进行有条件的聚合查询Aggregations,并输出前12条聚结果，以及前12条满足条件的doc
     * 例子： https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.6/_metrics_aggregations.html#java-aggs-metrics-tophits
     */
    @Test
    public void testQueryAggregation(){

        SearchRequestBuilder sbuilder = client.prepareSearch(testIndext).setTypes(testType);

        AggregationBuilder aggregation = AggregationBuilders
            .terms("agg").field("class")
            .subAggregation(
                    AggregationBuilders.topHits("top")
                            .explain(true)
                            .size(12)
                            .from(0)
            );
        sbuilder.addAggregation(aggregation);
        SearchResponse response = sbuilder.execute().actionGet();

        // sr is here your SearchResponse object
        Terms agg = response.getAggregations().get("agg");

        // For each entry
        for (Terms.Bucket entry : agg.getBuckets()) {
            String key = entry.getKey().toString();                    // bucket key
            long docCount = entry.getDocCount();            // Doc count
            System.out.println("key ["+key+"], doc_count ["+docCount+"]");

            // We ask for top_hits for each bucket
            TopHits topHits = entry.getAggregations().get("top");
            for (SearchHit hit : topHits.getHits().getHits()) {
                System.out.println(" -> id ["+hit.getId()+"], _source ["+hit.getSourceAsString()+"]");
            }
        }

    }

    /**
     * 查询open或者close的索引
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.6/java-admin-cluster.html#java-admin-cluster-health
     */
    @Test
    public void testQueryIndexStatus(){

        ImmutableOpenMap<String, IndexMetaData>  indexMetas =  client.admin().cluster().prepareState().execute().actionGet()
                .getState().getMetaData().indices();

        for(ObjectCursor<IndexMetaData> indexMeta : indexMetas.values()){
            System.out.println("index="+indexMeta.value.getIndex().getName()+" state="+indexMeta.value.getState().id()+" (0=open, 1=close)");
        }


/*        ActionFuture<IndicesStatsResponse> isr = client.admin().indices().stats(new IndicesStatsRequest().all());
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        Map<String, IndexStats> indexStatsMap = isr.actionGet().getIndices();
        Set<String> set = isr.actionGet().getIndices().keySet();*/


        /*        AdminClient adminClient = client.admin();
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        adminClient.indices().prepareGetIndex().get().indices();*/

/*        ClusterHealthResponse healths = client.admin().cluster().prepareHealth().get();
        String clusterName = healths.getClusterName();
        int numberOfDataNodes = healths.getNumberOfDataNodes();
        int numberOfNodes = healths.getNumberOfNodes();

        for (ClusterIndexHealth health : healths.getIndices().values()) {
            String index = health.getIndex();
            int numberOfShards = health.getNumberOfShards();
            int numberOfReplicas = health.getNumberOfReplicas();
            ClusterHealthStatus status = health.getStatus();
            System.out.print("");
        }*/

    }







}
