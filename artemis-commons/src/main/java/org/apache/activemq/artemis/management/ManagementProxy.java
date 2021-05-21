package org.apache.activemq.artemis.management;

public interface ManagementProxy extends AutoCloseable {
   void connect() throws Exception;

   void disconnect() throws Exception;

   Object getAttribute(String resourceName, String attributeName, int timeout) throws Exception;

   Object invokeOperation(String resourceName, String operationName, Object[] operationParams, int timeout) throws Exception;
}
