/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.reload;

import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.utils.XMLUtil;
import org.jboss.logging.Logger;

public class ReloadManagerImpl extends ActiveMQScheduledComponent implements ReloadManager {

   private static final Logger logger = Logger.getLogger(ReloadManagerImpl.class);

   private volatile Runnable tick;

   private final Map<URL, ReloadRegistry> registry = new HashMap<>();

   private ReloadCheckType checkType;

   public ReloadManagerImpl(ScheduledExecutorService scheduledExecutorService, Executor executor, long checkPeriod, ReloadCheckType checkType) {
      super(scheduledExecutorService, executor, checkPeriod, TimeUnit.MILLISECONDS, false);
      this.checkType = checkType;
   }

   @Override
   public void run() {
      tick();
   }

   @Override
   public synchronized void setTick(Runnable tick) {
      this.tick = tick;
   }

   @Override
   public synchronized void addCallback(URL uri, ReloadCallback callback) {
      if (!isStarted()) {
         start();
      }
      ReloadRegistry uriRegistry = getRegistry(uri);
      uriRegistry.add(callback);
   }

   private synchronized void tick() {
      for (ReloadRegistry item : registry.values()) {
         item.check();
      }

      if (tick != null) {
         tick.run();
         tick = null;
      }
   }

   private ReloadRegistry getRegistry(URL uri) {
      ReloadRegistry uriRegistry = registry.get(uri);
      if (uriRegistry == null) {
         uriRegistry = new ReloadRegistry(uri);
         registry.put(uri, uriRegistry);
      }

      return uriRegistry;
   }

   class ReloadRegistry {

      private File file;
      private final URL uri;
      private byte[] lastDigest;
      private long lastModified;

      private final List<ReloadCallback> callbacks = new LinkedList<>();

      ReloadRegistry(URL uri)  {
         try {
            file = new File(uri.toURI()); // artemis-features will have this as "file:etc/artemis.xml"
                                          // so, we need to make sure we catch the exception and try
                                          // a simple path as it will be a relative path
         } catch (Exception e) {
            logger.debug(e.getMessage(), e);
            file = new File(uri.getPath());
         }

         if (!file.exists()) {
            ActiveMQServerLogger.LOGGER.fileDoesNotExist(file.toString());
         }

         this.uri = uri;
         this.lastDigest = null;
         this.lastModified = 0;

         this.check();
      }

      public void check() {

         boolean reload = false;

         if (checkType == ReloadCheckType.LAST_MODIFIED) {
            long fileModified = file.lastModified();

            if (logger.isDebugEnabled()) {
               logger.debug("Validating lastModified " + lastModified + " modified = " + fileModified + " on " + uri);
            }

            reload = lastModified > 0 && fileModified > lastModified;

            this.lastModified = fileModified;
         } else if (checkType == ReloadCheckType.MD5_DIGEST) {
            try {
               MessageDigest md5MessageDigest = MessageDigest.getInstance("MD5");

               if (this.uri.getPath().toUpperCase().endsWith("XML")) {
                  try (InputStream inputStream = new FileInputStream(file);
                       DigestOutputStream digestOutputStream = new DigestOutputStream(new OutputStream() {
                          @Override public void write(int b) throws IOException { } }, md5MessageDigest)) {
                     StreamResult streamResult = new StreamResult(digestOutputStream);
                     TransformerFactory.newInstance().newTransformer().transform(
                        new DOMSource(XMLUtil.streamToElement(inputStream)), streamResult);
                  }
               } else {
                  byte[] buffer = new byte[1024];
                  try (DigestInputStream digestInputStream = new DigestInputStream(new FileInputStream(file), md5MessageDigest)) {
                     while (digestInputStream.read(buffer) != -1) {
                        // Read the input stream until EOF.
                     }
                  }
               }

               byte[] fileDigest = md5MessageDigest.digest();

               if (logger.isDebugEnabled()) {
                  logger.debug("Validating lastDigest " + lastDigest + " modified = " + fileDigest + " on " + uri);
               }

               reload = lastDigest != null && !Arrays.equals(lastDigest, fileDigest);

               lastDigest = fileDigest;
            } catch (Throwable e) {
               ActiveMQServerLogger.LOGGER.configurationReloadCheckFailed(e);
            }
         }

         if (reload) {
            for (ReloadCallback callback : callbacks) {
               try {
                  callback.reload(uri);
               } catch (Throwable e) {
                  ActiveMQServerLogger.LOGGER.configurationReloadFailed(e);
               }
            }
         }
      }

      public List<ReloadCallback> getCallbacks() {
         return callbacks;
      }

      public void add(ReloadCallback callback) {
         callbacks.add(callback);
      }
   }
}
