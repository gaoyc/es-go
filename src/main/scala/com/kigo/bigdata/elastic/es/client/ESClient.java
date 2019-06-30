package com.kigo.bigdata.elastic.es.client;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by kigo on 18-1-16.
 */
public class ESClient {

    private static TransportClient client;

    private static String clusterName = "elasticsearch";
    private static String hostName = "localhost";
    private static int port = 9300;

    public static TransportClient getClient() {
        if(client!=null){
            return client;
        }
        try {

            // 通过map参数构建Settings
/*            Map<String, String> map = new HashMap<String, String>();
            map.put("cluster.name", clusterName);
            Settings.Builder settings = Settings.builder().put(map);*/

            // 通过setting对象指定集群配置信息, 配置的集群名
            Settings settings = Settings.builder().put("cluster.name", clusterName) // 设置集群名; Settings.settingsBuilder
//                .put("client.transport.sniff", true) // 开启嗅探 , 开启后会一直连接不上, 原因未知
//                .put("network.host", "127.0.0.1")
                    .put("client.transport.ignore_cluster_name", true) // 忽略集群名字验证, 打开后集群名字不对也能连接上
//                .put("client.transport.nodes_sampler_interval", 5) //报错,
//                .put("client.transport.ping_timeout", 5) // 报错, ping等待时间,
                    .build();


            // 旧版本API
            //client = TransportClient.builder().settings(settings).build()
            //      .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("192.168.50.37", 9300)));

            //5.x.x 版本API
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), port)) //可持续添加多个节点
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), port));

            System.out.println("success connect");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }


    public  SearchResponse matchQuery(String indices,String field,String queryString){
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
        return searchResponse;
    }


}
