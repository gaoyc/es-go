package com.kigo.bigdata.elastic.es.demo;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
/**
 * Created by kigo on 18-1-16.
 */
public class TransportClientTest {


    private static TransportClient client;

    private static String clusterName = "elasticsearch";
    private static String hostName = "localhost";
    private  static int port = 9300;

    @SuppressWarnings("resource")
    public static TransportClient getClient() {
        if(client!=null){
            return client;
        }
        try {
            Settings settings = Settings.builder().put("cluster.name", clusterName)
                    .put("client.transport.sniff", true).build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), port));
                    //.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.7.204"), 9303));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;


    }


    public static void main(String[] args) {
        for(int i=0;i<100;i++){
            index();
            search();
        }
    }


    public static void search(){
        TransportClient client=getClient();
        SearchResponse response = client.prepareSearch("users")
                    /*      .setTypes("user")
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setQuery(QueryBuilders.termQuery("multi", "test"))
                            .setPostFilter(QueryBuilders.rangeQuery("age").from(0).to(18))  */
                .setFrom(0).setSize(60).setExplain(true)
                .get();
        System.out.println(response);

    }

    public static void index(){
        TransportClient client = getClient();
        Map<String, Object> map = new HashMap<String, Object>();
        Random ran = new Random();
        map.put("nickname", "测试" + ran.nextInt(100));
        map.put("sex", ran.nextInt(100));
        map.put("age", ran.nextInt(100));
        map.put("mobile", "15014243232");
        IndexResponse response = client.prepareIndex("users", "user").setSource(map).get();
        System.out.println(response);
    }
}

