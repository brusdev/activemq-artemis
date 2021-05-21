package org.apache.activemq.artemis.management;

public interface ManagementProxyFactory {
   public abstract String[] getSupportedProtocols();

   ManagementProxy createProxy(String protocol, String url, String user, String password);
}
