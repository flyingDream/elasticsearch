package com.huan.elasticsearch.helper;

import java.io.IOException;
import java.net.InetAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class EsClient的创建 {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
		Settings settings = Settings.builder()//
				.put("cluster.name", "elasticsearch")// 设置es集群的名字
				.put("client.transport.sniff", true)// 设置集群中的节点自动嗅探 比如一个es集群有100个节点，我们下方连集群的时候难道要写100个节点的地址，这个就不好了，此时开启嗅探功能，那么
				// 只需要些几个节点即可，会自动发现其余的节点并加入集群，默认每5秒查询一次
				.build();
		TransportClient transportClient = new PreBuiltTransportClient(settings)//
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.99.1"), 9300));

		System.out.println(transportClient);

		transportClient.close();
	}
}
