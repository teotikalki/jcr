/*
 * Copyright (C) 2014 eXo Platform SAS.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.exoplatform.services.jcr.impl.storage.value.gfs;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import org.exoplatform.services.jcr.impl.dataflow.SpoolConfig;
import org.exoplatform.services.jcr.impl.dataflow.ValueDataUtil.ValueDataWrapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.jcr.PropertyType;

/**
 * Tests related to the class {@link GridFSValueUtil}.
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class TestGridFSValueUtil
{
   private GridFS gridFs;
   private MongoClient mongo;

   @Before
   public void createGridFS() throws Exception
   {
      Properties properties = new Properties();
      properties.load(TestGridFSValueUtil.class.getResourceAsStream("/conf/standalone/config.properties"));
      String connectionURI = properties.getProperty("connection-uri");
      MongoClientURI uri = new MongoClientURI(connectionURI);
      String databaseName = uri.getDatabase() == null || uri.getDatabase().isEmpty() ? "jcr" : uri.getDatabase();
      this.mongo = new MongoClient(uri);
      DB mongoDb = mongo.getDB(databaseName);
      this.gridFs = new GridFS(mongoDb, "TestGridFSValueUtil");
   }

   @After
   public void destroyGridFS() throws Exception
   {
      if (mongo != null)
      {
         gridFs.getFilesCollection().drop();
         gridFs.getChunkCollection().drop();
         mongo.close();
      }
   }

   @Test
   public void testAll() throws Exception
   {
      Assert.assertNotNull(gridFs);
      int total = 12;
      String content = "This is the content of my value !";
      long length = content.length();
      for (int i = 0; i < total; i++)
      {
         GridFSValueUtil.writeValue(gridFs, "foo" + i, "foo", new ByteArrayInputStream(content.getBytes("UTF-8")));
      }
      SpoolConfig config = SpoolConfig.getDefaultSpoolConfig();
      for (int i = 0; i < total; i++)
      {
         // length <= spoolConfig.maxBufferSize without trying to access to the content 
         GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config);
      }
      for (int i = 0; i < total; i++)
      {
         // length <= spoolConfig.maxBufferSize, access to the content without reading it using getAsStream
         GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config).value.getAsStream();
      }
      for (int i = 0; i < total; i++)
      {
         // length <= spoolConfig.maxBufferSize, access to the content without reading it using getAsByteArray
         GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config).value.getAsByteArray();
      }
      for (int i = 0; i < total; i++)
      {
         // length <= spoolConfig.maxBufferSize, access to the content without reading it
         GridFSValueUtil.getContent(gridFs, "foo" + i, config);
      }
      for (int i = 0; i < total; i++)
      {
         // length <= spoolConfig.maxBufferSize, access to the content and read it using getAsStream
         ValueDataWrapper wrapper = GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config);
         Assert.assertEquals(length, wrapper.size);
         InputStream io = wrapper.value.getAsStream();
         Assert.assertEquals(length, wrapper.size);
         byte[] buffer = new byte[128];
         int l = io.read(buffer);
         Assert.assertEquals(-1, io.read());
         io.close();
         Assert.assertEquals(content, new String(buffer, 0, l, "UTF-8"));
      }
      for (int i = 0; i < total; i++)
      {
         // length <= spoolConfig.maxBufferSize, access to the content and read it using getAsByteArray
         ValueDataWrapper wrapper = GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config);
         Assert.assertEquals(length, wrapper.size);
         byte[] result = wrapper.value.getAsByteArray();
         Assert.assertEquals(length, wrapper.size);
         Assert.assertEquals(content, new String(result, "UTF-8"));
      }
      for (int i = 0; i < total; i++)
      {
         // length <= spoolConfig.maxBufferSize, access to the content and read it
         InputStream io = GridFSValueUtil.getContent(gridFs, "foo" + i, config);
         byte[] buffer = new byte[128];
         int l = io.read(buffer);
         Assert.assertEquals(-1, io.read());
         io.close();
         Assert.assertEquals(content, new String(buffer, 0, l, "UTF-8"));
      }
      for (int i = 0; i < total; i++)
      {
         // length <= spoolConfig.maxBufferSize, access to the content and read it using getAsStream without closing the stream
         InputStream io = GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config).value.getAsStream();
         byte[] buffer = new byte[128];
         int l = io.read(buffer);
         Assert.assertEquals(-1, io.read());
         Assert.assertEquals(content, new String(buffer, 0, l, "UTF-8"));
      }
      for (int i = 0; i < total; i++)
      {
         // length <= spoolConfig.maxBufferSize, access to the content and read it without closing the stream
         InputStream io = GridFSValueUtil.getContent(gridFs, "foo" + i, config);
         byte[] buffer = new byte[128];
         int l = io.read(buffer);
         Assert.assertEquals(-1, io.read());
         Assert.assertEquals(content, new String(buffer, 0, l, "UTF-8"));
      }
      config = SpoolConfig.getDefaultSpoolConfig();
      config.maxBufferSize = 16;
      for (int i = 0; i < total; i++)
      {
         // length > spoolConfig.maxBufferSize without trying to access to the content 
         GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config);
      }
      for (int i = 0; i < total; i++)
      {
         // length > spoolConfig.maxBufferSize, access to the content without reading it using getAsStream
         GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config).value.getAsStream();
      }
      for (int i = 0; i < total; i++)
      {
         // length > spoolConfig.maxBufferSize, access to the content without reading it using getAsByteArray
         GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config).value.getAsByteArray();
      }
      for (int i = 0; i < total; i++)
      {
         // length > spoolConfig.maxBufferSize, access to the content without reading it
         GridFSValueUtil.getContent(gridFs, "foo" + i, config);
      }
      for (int i = 0; i < total; i++)
      {
         // length > spoolConfig.maxBufferSize, access to the content and read it using getAsStream
         ValueDataWrapper wrapper = GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config);
         Assert.assertEquals(length, wrapper.size);
         InputStream io = wrapper.value.getAsStream();
         Assert.assertEquals(length, wrapper.size);
         byte[] buffer = new byte[128];
         int l = io.read(buffer);
         Assert.assertEquals(-1, io.read());
         io.close();
         Assert.assertEquals(content, new String(buffer, 0, l, "UTF-8"));
      }
      for (int i = 0; i < total; i++)
      {
         // length > spoolConfig.maxBufferSize, access to the content and read it using getAsByteArray
         ValueDataWrapper wrapper = GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config);
         Assert.assertEquals(length, wrapper.size);
         byte[] result = wrapper.value.getAsByteArray();
         Assert.assertEquals(length, wrapper.size);
         Assert.assertEquals(content, new String(result, "UTF-8"));
      }
      for (int i = 0; i < total; i++)
      {
         // length > spoolConfig.maxBufferSize, access to the content and read it
         InputStream io = GridFSValueUtil.getContent(gridFs, "foo" + i, config);
         byte[] buffer = new byte[128];
         int l = io.read(buffer);
         Assert.assertEquals(-1, io.read());
         io.close();
         Assert.assertEquals(content, new String(buffer, 0, l, "UTF-8"));
      }
      for (int i = 0; i < total; i++)
      {
         // length > spoolConfig.maxBufferSize, access to the content and read it using getAsStream without closing the stream
         InputStream io = GridFSValueUtil.readValueData(gridFs, "foo" + i, PropertyType.BINARY, 1, config).value.getAsStream();
         byte[] buffer = new byte[128];
         int l = io.read(buffer);
         Assert.assertEquals(-1, io.read());
         Assert.assertEquals(content, new String(buffer, 0, l, "UTF-8"));
      }
      for (int i = 0; i < total; i++)
      {
         // length > spoolConfig.maxBufferSize, access to the content and read it without closing the stream
         InputStream io = GridFSValueUtil.getContent(gridFs, "foo" + i, config);
         byte[] buffer = new byte[128];
         int l = io.read(buffer);
         Assert.assertEquals(-1, io.read());
         Assert.assertEquals(content, new String(buffer, 0, l, "UTF-8"));
      }
      for (int i = 0; i < total; i++)
      {
         Assert.assertTrue(GridFSValueUtil.exists(gridFs, "foo" + i));
      }
      for (int i = 0; i < total; i++)
      {
         Assert.assertEquals(length, GridFSValueUtil.getContentLength(gridFs, "foo" + i));
      }
      Assert.assertEquals(length * total, GridFSValueUtil.getFullContentLength(gridFs, "foo"));
      for (int i = 0; i < total; i++)
      {
         Assert.assertTrue(GridFSValueUtil.delete(gridFs, "foo" + i));
      }
      for (int i = 0; i < total; i++)
      {
         Assert.assertFalse(GridFSValueUtil.exists(gridFs, "foo" + i));
      }
      for (int i = 0; i < total; i++)
      {
         Assert.assertEquals(-1, GridFSValueUtil.getContentLength(gridFs, "foo" + i));
      }
      Assert.assertEquals(-1, GridFSValueUtil.getFullContentLength(gridFs, "foo"));
      for (int i = 0; i < total; i++)
      {
         GridFSValueUtil.writeValue(gridFs, "foo" + i, "foo", new ByteArrayInputStream(content.getBytes("UTF-8")));
      }
      for (int i = 0; i < total; i++)
      {
         List<String> keys = GridFSValueUtil.getKeys(gridFs, "bar");
         Assert.assertEquals(0, keys.size());
      }
      for (int i = 0; i < total; i++)
      {
         List<String> keys = GridFSValueUtil.getKeys(gridFs, "foo");
         Assert.assertEquals(total, keys.size());
         for (int j = 0; j < total; j++)
         {
            keys.remove("foo" + j);
         }
         Assert.assertEquals(0, keys.size());
      }
      String key1 = null, key2 = null;
      for (int i = 0; i < total; i++)
      {
         if (key1 == null)
         {
            key1 = "foo" + i;
         }
         else if (key2 == null)
         {
            key2 = "foo" + i;
         }
         if (key1 != null && key2 != null)
         {
            List<String> result = GridFSValueUtil.delete(gridFs, Arrays.asList(key1, key2));
            Assert.assertNull(result);
            key1 = null;
            key2 = null;
         }
      }
      for (int i = 0; i < total; i++)
      {
         Assert.assertFalse(GridFSValueUtil.exists(gridFs, "foo" + i));
      }
      for (int i = 0; i < total; i++)
      {
         Assert.assertEquals(-1, GridFSValueUtil.getContentLength(gridFs, "foo" + i));
      }
      Assert.assertEquals(-1, GridFSValueUtil.getFullContentLength(gridFs, "foo"));
      key1 = null;
      key2 = null;
      for (int i = 0; i < total; i++)
      {
         if (key1 == null)
         {
            key1 = "foo" + i;
         }
         else if (key2 == null)
         {
            key2 = "foo" + i;
         }
         if (key1 != null && key2 != null)
         {
            List<String> result = GridFSValueUtil.delete(gridFs, Arrays.asList(key1, key2));
            Assert.assertNull(result);
            key1 = null;
            key2 = null;
         }
      }
   }
}
