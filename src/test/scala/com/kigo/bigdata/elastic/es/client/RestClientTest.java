package com.kigo.bigdata.elastic.es.client;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * 官方参考文档： https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/index.html
 * Created by kigo on 18-2-27.
 */
public class RestClientTest {

    RestClient restClient = RestClient.builder(
            new HttpHost("localhost", 9200, "http"),
            new HttpHost("localhost", 9201, "http")).build();

    /**
     * 测试样例参考： https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html
     * @throws Exception
     */
    @Test
    public void testPerformRequest() throws Exception {
        Response response = restClient.performRequest("GET", "/");

        //测试get操作，带参数
        Map<String, String> params = Collections.singletonMap("pretty", "true");
        Response response2 = restClient.performRequest("GET", "/", params);


        //测试PUT操作
        Map<String, String> params3 = Collections.emptyMap();
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response3 = restClient.performRequest("PUT", "/posts/doc/1", params3, entity);

        //测试GET操作
        Map<String, String> params4 = Collections.emptyMap();
        HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory consumerFactory =
                new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(30 * 1024 * 1024);
        Response response4 = restClient.performRequest("GET", "/posts/_search", params4, null, consumerFactory);


        //测试异步响应
        ResponseListener responseListener = new ResponseListener() {
            @Override
            public void onSuccess(Response response) {

            }

            @Override
            public void onFailure(Exception exception) {

            }
        };
        restClient.performRequestAsync("GET", "/", responseListener);



        String jsonString5 = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        HttpEntity entity5 = new NStringEntity(jsonString5, ContentType.APPLICATION_JSON);
        restClient.performRequestAsync("PUT", "/posts/doc/1", params, entity, responseListener);


        //
        HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory consumerFactory6 =
                new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(30 * 1024 * 1024);
        restClient.performRequestAsync("GET", "/posts/_search", params, null, consumerFactory6, responseListener);


        HttpEntity[] documents = {entity, entity5};
        final CountDownLatch latch = new CountDownLatch(documents.length);
        for (int i = 0; i < documents.length; i++) {
            restClient.performRequestAsync(
                    "PUT",
                    "/posts/doc/" + i,
                    Collections.<String, String>emptyMap(),
                    //let's assume that the documents are stored in an HttpEntity array
                    documents[i],
                    new ResponseListener() {
                        @Override
                        public void onSuccess(Response response) {

                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Exception exception) {

                            latch.countDown();
                        }
                    }
            );
        }
        latch.await();




        Response response7 = restClient.performRequest("GET", "/", new BasicHeader("header", "value"));

        Header[] headers = {
                new BasicHeader("header1", "value1"),
                new BasicHeader("header2", "value2")
        };
        restClient.performRequestAsync("GET", "/", responseListener, headers);

    }

    /**
     * 参考自： https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-responses.html
     * @throws Exception
     */
    @Test
    public void testReadRespone() throws Exception {

        Response response = restClient.performRequest("GET", "/");
        RequestLine requestLine = response.getRequestLine();
        HttpHost host = response.getHost();
        int statusCode = response.getStatusLine().getStatusCode();
        Header[] headers = response.getHeaders();

        //The response body enclosed in an org.apache.http.HttpEntity object
        String responseBody = EntityUtils.toString(response.getEntity());

    }

    @After
    public void after() throws Exception {
            restClient.close();
    }
}
