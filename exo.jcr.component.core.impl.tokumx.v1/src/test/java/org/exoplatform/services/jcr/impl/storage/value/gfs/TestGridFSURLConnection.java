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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Tests related to the class {@link GridFSURLConnection}.
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class TestGridFSURLConnection
{
   private static final String CONTENT = "This is the content of my value for the test TestGridFSURLCon...!";

   private GridFS gridFs;
   private MongoClient mongo;
   private GridFSURLConnection url;

   @Before
   public void init() throws Exception
   {
      Properties properties = new Properties();
      properties.load(TestGridFSValueUtil.class.getResourceAsStream("/conf/standalone/config.properties"));
      String connectionURI = properties.getProperty("connection-uri");
      MongoClientURI uri = new MongoClientURI(connectionURI);
      String databaseName = uri.getDatabase() == null || uri.getDatabase().isEmpty() ? "jcr" : uri.getDatabase();
      this.mongo = new MongoClient(uri);
      DB mongoDb = mongo.getDB(databaseName);
      this.gridFs = new GridFS(mongoDb, "TestGridFSURLConnection");
      this.url = new GridFSURLConnection(gridFs, "foo", null);
      GridFSValueUtil.writeValue(gridFs, url.getIdResource(), url.getIdResource(), new ByteArrayInputStream(CONTENT.getBytes("UTF-8")));
   }

   @After
   public void destroy() throws Exception
   {
      if (url != null)
         GridFSValueUtil.delete(gridFs, url.getIdResource());
      if (mongo != null)
      {
         gridFs.getFilesCollection().drop();
         gridFs.getChunkCollection().drop();
         mongo.close();
      }
   }

   @Test
   public void testGetContentLength() throws Exception
   {
      Assert.assertEquals(CONTENT.length(), url.getContentLength());
   }

   @Test
   public void testGetInputStream() throws Exception
   {
      Assert.assertEquals(CONTENT.length(), url.getInputStream().available());
      InputStream io = url.getInputStream();
      byte[] buffer = new byte[128];
      int l = io.read(buffer);
      Assert.assertEquals(-1, io.read(buffer));
      Assert.assertEquals(0, io.available());
      io.close();
      Assert.assertEquals(CONTENT, new String(buffer, 0, l, "UTF-8"));
      io = url.getInputStream();
      buffer = new byte[128];
      for (int i = 0; i < l; i++)
      {
         int r = io.read();
         Assert.assertFalse(r == -1);
         Assert.assertEquals(l - i - 1, io.available());
         buffer[i] = (byte)r;
      }
      Assert.assertEquals(-1, io.read());
      Assert.assertEquals(0, io.available());
      io.close();
      Assert.assertEquals(CONTENT, new String(buffer, 0, l, "UTF-8"));
      io = url.getInputStream();
      buffer = new byte[13];
      int remaining = CONTENT.length();
      Assert.assertEquals(remaining, io.available());
      Assert.assertEquals(13, io.read(buffer));
      Assert.assertEquals(remaining -= 13, io.available());
      Assert.assertEquals(13, io.read(buffer));
      Assert.assertEquals(remaining -= 13, io.available());
      Assert.assertEquals(13, io.read(buffer));
      Assert.assertEquals(remaining -= 13, io.available());
      Assert.assertEquals(13, io.read(buffer));
      Assert.assertEquals(remaining -= 13, io.available());
      Assert.assertEquals(13, io.read(buffer));
      Assert.assertEquals(0, io.available());
      Assert.assertEquals(-1, io.read(buffer));
      io.close();

      io = url.getInputStream();
      Assert.assertEquals(CONTENT.length(), io.available());
      io.skip(13);
      Assert.assertEquals(CONTENT.length() - 13, io.available());
      buffer = new byte[128];
      int l2 = io.read(buffer);
      Assert.assertEquals(-1, io.read(buffer));
      Assert.assertEquals(0, io.available());
      io.close();
      Assert.assertEquals(CONTENT.substring(13), new String(buffer, 0, l2, "UTF-8"));
      io = url.getInputStream();
      io.skip(13);
      buffer = new byte[128];
      for (int i = 0; i < l2; i++)
      {
         int r = io.read();
         Assert.assertFalse(r == -1);
         Assert.assertEquals(l2 - i - 1, io.available());
         buffer[i] = (byte)r;
      }
      Assert.assertEquals(-1, io.read());
      Assert.assertEquals(0, io.available());
      io.close();
      Assert.assertEquals(CONTENT.substring(13), new String(buffer, 0, l2, "UTF-8"));
      io = url.getInputStream();
      buffer = new byte[13];
      remaining = CONTENT.length();
      Assert.assertEquals(remaining, io.available());
      io.skip(13);
      Assert.assertEquals(remaining -= 13, io.available());
      Assert.assertEquals(13, io.read(buffer));
      Assert.assertEquals(remaining -= 13, io.available());
      Assert.assertEquals(13, io.read(buffer));
      Assert.assertEquals(remaining -= 13, io.available());
      Assert.assertEquals(13, io.read(buffer));
      Assert.assertEquals(remaining -= 13, io.available());
      Assert.assertEquals(13, io.read(buffer));
      Assert.assertEquals(0, io.available());
      Assert.assertEquals(-1, io.read(buffer));
      io.close();
   }
}
