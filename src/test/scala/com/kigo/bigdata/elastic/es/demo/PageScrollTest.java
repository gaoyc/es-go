package com.kigo.bigdata.elastic.es.demo;

import com.kigo.bigdata.elastic.es.client.ESClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

import java.util.Date;

/**
 * Created by kigo on 18-2-9.
 */
public class PageScrollTest {

    Log log = LogFactory.getLog(ESClientCrudTest.class);
    TransportClient client = ESClient.getClient();

    private String INDEX = "index_test";
    private String TYPE = "type_test";

     @Test
    public void testPageByFromSize() {
         System.out.println("from size 模式启动！");
         Date begin = new Date();
         //long count = client.prepareCount(INDEX).setTypes(TYPE).execute().actionGet().getCount(); //旧版本API
         long count = client.prepareSearch(INDEX).setTypes(TYPE).execute().actionGet().getHits().getTotalHits();
         SearchRequestBuilder requestBuilder = client.prepareSearch(INDEX).setTypes(TYPE).
                 setQuery(QueryBuilders.matchAllQuery());
         for(int i=0,sum=0; sum<count; i++){
             SearchResponse response = requestBuilder.setFrom(i).setSize(50000).
                     execute().actionGet();
             sum += response.getHits().hits().length;
             System.out.println("总量"+count+" 已经查到"+sum);
         }
         Date end = new Date();
         System.out.println("耗时: "+(end.getTime()-begin.getTime()));
    }

    @Test
    public void testPageByScroll() {
        System.out.println("scroll 模式启动！");
        Date begin = new Date();
        SearchResponse scrollResponse = client.prepareSearch(INDEX)
                .setSearchType(SearchType.DEFAULT).setSize(10000).setScroll(TimeValue.timeValueMinutes(1))
                .execute().actionGet();
        Long count = scrollResponse.getHits().getTotalHits();//第一次不返回数据
        for(int i=0,sum=0; sum<count; i++){
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(8))
                    .execute().actionGet();
            sum += scrollResponse.getHits().hits().length;
            System.out.println("总量"+count+" 已经查到"+sum);
        }
        Date end = new Date();
        System.out.println("耗时: "+(end.getTime()-begin.getTime()));
    }

}
