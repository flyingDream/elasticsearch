package com.huan.elasticsearch.入门;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSON;

/**
 * 对员工进行增删该查操作
 * 
 * @描述
 * @作者 huan
 * @时间 2017年8月20日 - 下午8:58:16
 */
public class EmployeeCrud {

	public static void main(String[] args) throws UnknownHostException, InterruptedException, ExecutionException {
		Settings settings = Settings.builder()//
				.put("cluster.name", "elasticsearch")//
				.build();
		@SuppressWarnings("resource")
		TransportClient transportClient = new PreBuiltTransportClient(settings)//
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.99.1"), 9300));
		// 创建索引(transportClient);
		// 创建文档(transportClient);
		// 查询文档(transportClient);
		// 修改文档(transportClient);
		// 删除文档(transportClient);
		删除索引(transportClient);
		transportClient.close();
	}

	public static void 删除索引(TransportClient transportClient) {
		transportClient.admin().indices().delete(new DeleteIndexRequest("indexemployee")).actionGet();
	}

	private static void 删除文档(TransportClient transportClient) {
		DeleteResponse actionGet = transportClient.prepareDelete("indexemployee", "type1", "1").execute().actionGet();
		System.out.println(actionGet);
	}

	private static void 修改文档(TransportClient transportClient) throws InterruptedException, ExecutionException {
		UpdateResponse updateResponse = transportClient.prepareUpdate("indexemployee", "type1", "1")//
				.setDoc("name", "fu huan update")//
				.execute()//
				.get();
		System.out.println(updateResponse);
	}

	private static void 查询文档(TransportClient transportClient) {
		GetResponse actionGet = transportClient.prepareGet("indexemployee", "type1", "1").get();
		System.out.println(actionGet.getSourceAsString());
	}

	private static void 创建文档(TransportClient transportClient) {
		IndexResponse actionGet = transportClient.prepareIndex("indexemployee", "type1", "1")//
				.setSource(JSON.toJSONString(new Employee("huan", 12)))//
				.execute()//
				.actionGet();
		System.out.println(actionGet);
	}

	private static void 创建索引(TransportClient transportClient) {
		CreateIndexRequest request = new CreateIndexRequest("indexemployee");

		CreateIndexResponse actionGet = transportClient.admin()//
				.indices()//
				.create(request)//
				.actionGet();
		if (actionGet.isAcknowledged()) {
			System.out.println("索引创建成功");
		} else {
			System.out.println("索引创建失败");
		}
	}

	static class Employee {
		public String name;
		public int age;

		public Employee() {
		}

		public Employee(String name, int age) {
			super();
			this.name = name;
			this.age = age;
		}

	}
}
