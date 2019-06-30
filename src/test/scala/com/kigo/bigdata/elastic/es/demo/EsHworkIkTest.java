package com.kigo.bigdata.elastic.es.demo;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.Tika;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
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
import org.elasticsearch.client.transport.TransportClient;
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
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.*;

import java.io.File;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * <p>Es客户端IK中文分词及查询测试<p/>
 * <p>  以下环境测试通过: <p/>
 * <p>     Es集群版本: 5.6.0 <p/>
 * Created by kigo on 18-1-9.
 */
public class EsHworkIkTest {

    Log log = LogFactory.getLog(EsHworkIkTest.class);

    private static TransportClient client;

    private static String clusterName = "elasticsearch";
    //private String hostName = "localhost";
    private static String hostName = "127.0.0.1";
    private static int port = 9300;

    private static String testIndext = "idx_demoik_test";
    private static String testType = "default";

    private static String filedIk = "message";
    private static String filedRange = "stores";



    /**
     * 获取连接
     * @throws Exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception {

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

    @AfterClass
    public static void afterClass() throws Exception {
        //deleteIndex(testIndext);
    }


    @Before
    public void before() throws Exception {

    }

    @After
    public void after() throws Exception {

    }

    @Test
    public void testIndexContentByTika() throws Exception {
        //XContentBuilder source = createIKJson4("中华人民共和国");
         String[] args = {"/tmp/tika-files/a.txt", "/tmp/tika-files/b.zip", "/tmp/tika-files/c.ppt", "/tmp/tika-files/d.doc"}; ///tmp/b.zip,"/tmp/b.doc"
         for (String file : args) {
             String content = getConetByTika(file);
             System.out.println("file="+file+" content="+content);
             XContentBuilder source = createIKJson4(content);
             // 存json入索引中
             IndexResponse response = client.prepareIndex(testIndext, testType).setSource(source).get();
//        // 结果获取
             String index = response.getIndex();
             String type = response.getType();
             String id = response.getId();
             long version = response.getVersion();
             DocWriteResponse.Result result = response.getResult();

             System.out.println("suc index to es, index="+index + ", type=" + type + ", id=" + id + ", version=" + version + ", result=" + result);
         }


    }


    /**
     * 范围查询
     */
    @Test
    public void testRangeQuery() {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(filedRange)
                .from(0)
                .to(100)
                .includeLower(true)     // 包含上界
                .includeUpper(true);      // 包含下届

        searchQueryBuilder(rangeQueryBuilder, testIndext);
    }


    /**
     *短语查询
     */
     @Test
    public void testPhraseQuery() {
         QueryBuilder phraseBuilder = QueryBuilders.matchPhraseQuery(filedIk, "共和");
         searchQueryBuilder(phraseBuilder, testIndext);
    }

    /**
     * 使用模糊相似度查询
     */
    @Test
    public void testFuzzyQuery() {
        QueryBuilder fuzzyQuery = QueryBuilders.fuzzyQuery(filedIk, "共和");
        searchQueryBuilder(fuzzyQuery, testIndext);
    }


    /**
     * 通配符查询
     */
    @Test
    public void testWildCardQuery() {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery(filedIk, "西*柿");
        searchQueryBuilder(queryBuilder, testIndext);
    }




    public  void searchQueryBuilder(QueryBuilder searchQueryBuilder, String... indices){
        SearchResponse searchResponse = client.prepareSearch(indices)
                .setQuery(searchQueryBuilder)
                .execute()
                .actionGet();
        SearchHits searchHits = searchResponse.getHits();
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
     * 使用es的帮助类生成测试doc
     */
    private XContentBuilder createIKJson4(String msg) throws Exception {
        // 创建json对象, 其中一个创建json的方式
        XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field(filedIk, msg)
                .field(filedRange, new Random().nextInt(100))
                //.field("user", "Kigo")
                //.field("postDate", new Date())
                //.field("class", new Random().nextInt(5))
                .endObject();
        return source;
    }


    public String getConetByTika(String file) throws Exception {
        // Create a Tika instance with the default configuration
        Tika tika = new Tika();
        String text = tika.parseToString(new File(file));
        return text.trim();
    }



    public static void deleteIndex(String index) throws ExecutionException, InterruptedException {
        DeleteIndexResponse response = client.admin().indices().prepareDelete(index).execute().get();
        boolean isSuc = response.isAcknowledged();
        System.out.println("delete the index of:"+index +" ret="+isSuc);
    }


}
