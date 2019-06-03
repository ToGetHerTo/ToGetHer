package com.chenyuxin;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author chenshiliu
 * @create 2019-06-03 13:58
 */

public class GetClient {
    private TransportClient client;
    /**
     * 创建 Transpot Client客户端的链接
     */
    public void getClient(){
        try {
            // 1 设置连接的集群名称
            Settings settings = Settings.builder().put("cluster.name", "集群名称").build();
            // 2 连接集群
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress
                            .getByName("集群地址"), 9300));
            // 3 打印集群名称
            //System.out.println(client.toString());
        }catch (Exception ex){
            client.close();
        }finally{
        }
    }

    /**
     * 创建索引
     */
    public void createIndex(){
        //创建索引
        client.admin().indices().prepareCreate("索引名(注意必须小写)").get();
        //关闭连接
        client.close();
    }

    /**
     *  删除索引
     */
    public void deleteIndex(){
        //删除索引
        client.admin().indices().prepareDelete("选择删除的索引名").get();
        //关闭连接
        client.close();
    }

    /**
     * 新建文档
     *      元数据json串
     *      当直接对Elasticsearch创建文档对象时，如果索引不存在，默认会自动创建，映射采用默认方式。
     */
    public void createJsonDocument(){
        //文档数据准备
        String json = "{" +
                "\"id\":\"文档编号\"" +
                "\"title\":\"基于lucene的搜索服务器\"" +
                "\"content\":\"基于RESTful web接口\"" +
                "}";
        //创建文档
        IndexResponse indexResponse = client
                .prepareIndex("索引名(切记必须小写)","类型(大小写都可以)","id")
                .setSource(json).execute().actionGet();
        //打印返回结果
        System.out.println("index" + indexResponse.getIndex());
        System.out.println("type" + indexResponse.getType());
        System.out.println("id" + indexResponse.getId());
        System.out.println("version" + indexResponse.getVersion());
        System.out.println("result" + indexResponse.getResult());
        //关闭连接
        client.close();

    }

    /**
     * 新建文档
     *      数据源map方式添加json
     */
    public void createMapDocument(){
        //文档数据准备
        Map<String,Object> json = new HashMap<String,Object>();
        json.put("id","id");
        json.put("title","基于lucene的搜索服务器");
        json.put("content","基于RESTful web接口");
        //创建文档
        IndexResponse indexResponse = client
                .prepareIndex("索引名(切记必须小写)","类型(大小写都可以)","id")
                .setSource(json).execute().actionGet();
        //打印返回结果
        System.out.println("index" + indexResponse.getIndex());
        System.out.println("type" + indexResponse.getType());
        System.out.println("id" + indexResponse.getId());
        System.out.println("version" + indexResponse.getVersion());
        System.out.println("result" + indexResponse.getResult());
        //关闭连接
        client.close();
    }

    /**
     * 新建文档
     *      元数据es构建器添加json
     */
    public void createESDocument() throws IOException {
        //通过es自带的帮助类，构建json数据
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("id","3")
                .field("title","基于lucene的搜索服务器")
                .field("content","基于RESTful web接口")
                .endObject();
        //创建文档
        IndexResponse indexResponse = client
                .prepareIndex("索引名(切记必须小写)","类型(大小写都可以)","id")
                .setSource(builder).get();
        //打印返回结果
        System.out.println("index" + indexResponse.getIndex());
        System.out.println("type" + indexResponse.getType());
        System.out.println("id" + indexResponse.getId());
        System.out.println("version" + indexResponse.getVersion());
        System.out.println("result" + indexResponse.getResult());
        //关闭连接
        client.close();
    }

    /**
     * 搜索文档数据
     *      单个索引
     */
    public void getDocument(){
        //查询文档
        GetResponse response = client.prepareGet("索引名(切记必须小写)","类型(大小写都可以)","id").get();
        //打印搜索结果
        System.out.println(response.getSourceAsString());
        //关闭连接
        client.close();
    }

    /**
     * 搜索文档数据
     *      多个索引
     */
    public void getMultDocument(){
        //查询多个文档
        MultiGetResponse responses = client.prepareMultiGet()
                .add("索引名(切记必须小写)","类型(大小写都可以)","id")
                .add("索引名(切记必须小写)","类型(大小写都可以)","id1","id2")
                .add("索引名(切记必须小写)","类型(大小写都可以)","id").get();
        //遍历返回的结果
        for (MultiGetItemResponse multiGetItemResponse : responses) {
            //获取查询的响应对象
            GetResponse getResponse = multiGetItemResponse.getResponse();
            //如果获取到查询结果
            if(getResponse.isExists()){
                String sourceAsString = getResponse.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }
        //关闭连接
        client.close();
    }

    /**
     * 更新文档数据
     *      update
     */
    public void updateDocument() throws IOException, ExecutionException, InterruptedException {
        //创建更新数据的请求对象
        UpdateRequest updateRequest = new UpdateRequest("索引名(切记必须小写)","类型(大小写都可以)","id");
        //updateRequest.index("索引名(切记必须小写)");
        //updateRequest.type("类型(大小写都可以)");
        //updateRequest.id("id");
        //设置文档对象
        updateRequest.doc(XContentFactory.jsonBuilder().startObject()
            //对没有的字段添加，对已有的字段替换
            .field("","")
            .field("","")
            .field("","")
            .endObject()
        );
        //获取跟新后的值
        UpdateResponse indexResponse = client.update(updateRequest).get();
        //打印返回结果
        System.out.println("index" + indexResponse.getIndex());
        System.out.println("type" + indexResponse.getType());
        System.out.println("id" + indexResponse.getId());
        System.out.println("version" + indexResponse.getVersion());
        System.out.println("result" + indexResponse.getResult());
        //关闭连接
        client.close();
    }

    /**
     * 更新文档数据
     *      upset
     */
    public void testUpsert() throws IOException, ExecutionException, InterruptedException {
        //设置查询条件，查找不到则添加
        IndexRequest indexRequest = new IndexRequest("索引名(切记必须小写)","类型(大小写都可以)","id")
                .source(XContentFactory.jsonBuilder().startObject()
                        .field("title","搜索服务器")
                        .field("content","安装使用方便").endObject()
                );
        //设置更新，查找到更新下面的设置
        UpdateRequest upsert = new UpdateRequest("索引名(切记必须小写)","类型(大小写都可以)","id")
                .doc(XContentFactory.jsonBuilder().startObject()
                        .field("user","李四")
                        .endObject()
                ).upsert(indexRequest);
        //获取跟新后的值
        UpdateResponse indexResponse = client.update(upsert).get();
        //打印返回结果
        System.out.println("index" + indexResponse.getIndex());
        System.out.println("type" + indexResponse.getType());
        System.out.println("id" + indexResponse.getId());
        System.out.println("version" + indexResponse.getVersion());
        System.out.println("result" + indexResponse.getResult());
        //关闭连接
        client.close();
    }

    /**
     * 删除文档数据
     */
    public void deleteDocument(){
        //删除文档
        DeleteResponse indexResponse = client
                .prepareDelete("索引名(切记必须小写)","类型(大小写都可以)","id").get();
        //打印结果
        System.out.println("index" + indexResponse.getIndex());
        System.out.println("type" + indexResponse.getType());
        System.out.println("id" + indexResponse.getId());
        System.out.println("version" + indexResponse.getVersion());
        System.out.println("result" + indexResponse.getResult());
        //关闭连接
        client.close();
    }

    /**
     * 条件查询-查询所有
     */
    public void matchAllQuery(){
        //执行查询
        SearchResponse searchResponse = client.prepareSearch("").setTypes()
                .setQuery(QueryBuilders.matchAllQuery()).get();
        //
    }
}

