package com.ryxc.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

public class RPCServer implements BizProtocol{
	
	public String hello(String name){
		System.out.println("我被调用了");
		return "hello " + name;
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final RPC.Builder builder = new RPC.Builder(conf);
		final Builder setProtocol = builder.setProtocol(BizProtocol.class);
		final Builder setInstance = setProtocol.setInstance(new RPCServer());
		final Builder setBindAddress = setInstance.setBindAddress("localhost");
		final Builder setPort = setBindAddress.setPort(8888);
		Server server = setPort.build();
		server.start();
	}

}
