package com.huan.elasticsearch.入门;

import java.net.InetAddress;
import java.util.stream.Stream;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSON;

public class EmployeeSearch {
	public static void main(String[] args) throws Exception {
		Settings settings = Settings.builder()//
				.put("cluster.name", "elasticsearch")//
				.build();
		@SuppressWarnings("resource")
		TransportClient transportClient = new PreBuiltTransportClient(settings)//
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.99.1"), 9300));
		// 增加数据(transportClient);
		// 搜索数据(transportClient);

		transportClient.close();
	}

	public static void 搜索数据(TransportClient transportClient) {
		SearchResponse actionGet = transportClient.prepareSearch("indexemployee")//
				.setTypes("type1")//
				.setQuery(QueryBuilders.matchQuery("name", "huan"))//
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH) //
				.setPostFilter(QueryBuilders.rangeQuery("age").gte(15).lte(25))//
				.highlighter(new HighlightBuilder().field("name", 150).preTags("<span stlye='color:red'>").postTags("</span>"))//
				.setFrom(0)//
				.setSize(10)//
				.execute()//
				.actionGet();
		System.out.println(actionGet);
		SearchHit[] searchHits = actionGet.getHits().getHits();
		Stream.of(searchHits).forEach((d) -> {
			System.out.println("data:" + d.getSourceAsString());
			System.out.println("高亮:" + d.getHighlightFields().get("name").getFragments()[0]);
			System.out.println("source:" + d.getSource());
			System.out.println("-----------------------");
		});

	}

	public static void 增加数据(TransportClient transportClient) {
		BulkProcessor bulkProcessor = BulkProcessor.builder(transportClient, new BulkProcessor.Listener() {
			@Override
			public void afterBulk(long arg0, BulkRequest arg1, BulkResponse arg2) {
				System.out.println("xxxxxxxxxxx");
			}

			@Override
			public void afterBulk(long arg0, BulkRequest arg1, Throwable arg2) {
				System.out.println("yyyyyyyyyyy");
			}

			@Override
			public void beforeBulk(long arg0, BulkRequest arg1) {
				System.out.println("zzzzzzzzzzzzz");
			}
		}).setBulkActions(1000)//
				.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)) //
				.setFlushInterval(TimeValue.timeValueSeconds(5)) //
				.setConcurrentRequests(0) // 0 表示同步 >0 表示可能使用多线程执行
				.setBackoffPolicy(//
						BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) //
				.build();
		bulkProcessor.add(new IndexRequest("indexemployee", "type1", "1").source(JSON.toJSONString(new Employee("fu huan", 20, "hu bei"))));
		bulkProcessor.add(new IndexRequest("indexemployee", "type1", "2").source(JSON.toJSONString(new Employee("fu huan", 18, "he nan"))));
		bulkProcessor.add(new IndexRequest("indexemployee", "type1", "3").source(JSON.toJSONString(new Employee("fu huan", 25, "he bei wuhan"))));
		bulkProcessor.add(new IndexRequest("indexemployee", "type1", "4").source(JSON.toJSONString(new Employee("fu huan", 30, "hu bei huanggang"))));
		bulkProcessor.add(new IndexRequest("indexemployee", "type1", "5").source(JSON.toJSONString(new Employee("fu huan", 15, "hu bei address"))));
		// // Flush any remaining requests
		bulkProcessor.flush();
		// // Or close the bulkProcessor if you don't need it anymore
		bulkProcessor.close();
		// // Refresh your indices
		transportClient.admin().indices().prepareRefresh().get();
	}

	static class Employee {
		public String name;
		public int age;
		public String address;

		public Employee(String name, int age, String address) {
			super();
			this.name = name;
			this.age = age;
			this.address = address;
		}

	}
}
