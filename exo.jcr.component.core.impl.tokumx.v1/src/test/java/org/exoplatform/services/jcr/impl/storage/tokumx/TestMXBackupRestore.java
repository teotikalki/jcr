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
package org.exoplatform.services.jcr.impl.storage.tokumx;

import org.exoplatform.services.jcr.JcrAPIBaseTest;
import org.exoplatform.services.jcr.dataflow.persistent.WorkspaceStorageCache;
import org.exoplatform.services.jcr.impl.Constants;
import org.exoplatform.services.jcr.impl.backup.Backupable;
import org.exoplatform.services.jcr.impl.backup.DataRestore;
import org.exoplatform.services.jcr.impl.backup.rdbms.DataRestoreContext;
import org.exoplatform.services.jcr.impl.core.SessionImpl;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;

import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;

/**
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class TestMXBackupRestore extends JcrAPIBaseTest
{
   private MXWorkspaceDataContainer container;

   private WorkspaceStorageCache cache;

   /**
    * {@inheritDoc}
    */
   public void setUp() throws Exception
   {
      super.setUp();

      container =
         (MXWorkspaceDataContainer)repository.getWorkspaceContainer("ws1").getComponent(MXWorkspaceDataContainer.class);

      cache = (WorkspaceStorageCache)repository.getWorkspaceContainer("ws1").getComponent(WorkspaceStorageCache.class);
   }

   /**
    * Backup operation.
    */
   public void testBackupRestoreClean() throws Exception
   {
      Session session = (SessionImpl)repository.login(credentials, "ws1");
      Node root = session.getRootNode();

      assertNotNull(container);
      assertNotNull(cache);

      File tempDir = new File("target/temp");

      assertFalse(root.hasNode("testBackupRestoreClean"));
      Node testRoot = root.addNode("testBackupRestoreClean");
      Node content1 = testRoot.addNode("test1").addNode("content");
      content1.addMixin("mix:referenceable");
      Node content2 = testRoot.addNode("test2").addNode("content").addNode("content");
      content2.addMixin("mix:referenceable");

      // Simple
      testRoot.setProperty("booleanT", true);
      testRoot.setProperty("booleanF", false);
      Calendar date = Calendar.getInstance();
      testRoot.setProperty("calendar", date);
      testRoot.setProperty("double", 1234.567);
      testRoot.setProperty("long", 123456789L);
      testRoot.setProperty("String", "foo");
      testRoot.setProperty("Binary", new ByteArrayInputStream("foo".getBytes(Constants.DEFAULT_ENCODING)));
      testRoot.setProperty("Node", content1);
      testRoot.setProperty("Name", session.getValueFactory().createValue(content1.getName()), PropertyType.NAME);
      testRoot.setProperty("Path", session.getValueFactory().createValue(content1.getPath()), PropertyType.PATH);

      // Multiple
      testRoot.setProperty("booleanM", new Value[]{session.getValueFactory().createValue(true),
         session.getValueFactory().createValue(false)}, PropertyType.BOOLEAN);
      testRoot.setProperty("calendarM", new Value[]{session.getValueFactory().createValue(date),
         session.getValueFactory().createValue(date)}, PropertyType.DATE);
      testRoot.setProperty("doubleM", new Value[]{session.getValueFactory().createValue(1234.567),
         session.getValueFactory().createValue(1234.567)}, PropertyType.DOUBLE);
      testRoot.setProperty("longM", new Value[]{session.getValueFactory().createValue(123456789L),
         session.getValueFactory().createValue(123456789L)}, PropertyType.LONG);
      testRoot.setProperty("StringM", new Value[]{session.getValueFactory().createValue("foo"),
         session.getValueFactory().createValue("bar")}, PropertyType.STRING);
      testRoot
         .setProperty(
            "BinaryM",
            new Value[]{
               session.getValueFactory().createValue(
                  new ByteArrayInputStream("foo".getBytes(Constants.DEFAULT_ENCODING))),
               session.getValueFactory().createValue(
                  new ByteArrayInputStream("bar".getBytes(Constants.DEFAULT_ENCODING)))}, PropertyType.BINARY);
      testRoot.setProperty("NodeM", new Value[]{session.getValueFactory().createValue(content1),
         session.getValueFactory().createValue(content2)}, PropertyType.REFERENCE);
      testRoot.setProperty("NameM", new Value[]{session.getValueFactory().createValue(content1.getName()),
         session.getValueFactory().createValue(content2.getName())}, PropertyType.REFERENCE);
      testRoot.setProperty("PathM", new Value[]{session.getValueFactory().createValue(content1.getPath()),
         session.getValueFactory().createValue(content2.getPath())}, PropertyType.REFERENCE);

      root.save();

      assertTrue(root.hasNode("testBackupRestoreClean"));
      checkProperties(testRoot, content1, content2, date);

      long initialNodeCount = container.getNodesCount();

      container.backup(tempDir);

      testRoot.remove();
      root.save();

      long nodeCountAfterDelete = container.getNodesCount();

      assertTrue(initialNodeCount > nodeCountAfterDelete);

      assertFalse(root.hasNode("testBackupRestoreClean"));

      DataRestoreContext context =
         new DataRestoreContext(new String[]{DataRestoreContext.STORAGE_DIR}, new Object[]{tempDir});
      DataRestore restorer = container.getDataRestorer(context);

      restorer.clean();

      assertEquals(0L, container.getNodesCount().longValue());

      restorer.restore();
      restorer.close();

      if (cache instanceof Backupable)
      {
         // clear cache to make sure that it will get everything from the data storage
         ((Backupable)cache).clean();
      }

      assertTrue(root.hasNode("testBackupRestoreClean"));
      assertEquals(initialNodeCount, container.getNodesCount().longValue());

      testRoot = root.getNode("testBackupRestoreClean");
      content1 = testRoot.getNode("test1/content");
      content2 = testRoot.getNode("test2/content/content");

      checkProperties(testRoot, content1, content2, date);

      testRoot.remove();
      session.save();
      assertFalse(root.hasNode("testBackupRestoreClean"));

      assertEquals(nodeCountAfterDelete, container.getNodesCount().longValue());

      // Re-add to make sure that it is still working
      testRoot = root.addNode("testBackupRestoreClean");

      testRoot.addNode("test1").addNode("content");
      testRoot.addNode("test2").addNode("content").addNode("content");
      root.save();
      assertTrue(root.hasNode("testBackupRestoreClean"));
      assertEquals(initialNodeCount, container.getNodesCount().longValue());

      // Re-delete to make sure the behavior is consistent
      root.getNode("testBackupRestoreClean").remove();
      session.save();
      assertFalse(root.hasNode("testBackupRestoreClean"));

      assertEquals(nodeCountAfterDelete, container.getNodesCount().longValue());
      session.logout();
   }

   private void checkProperties(Node testRoot, Node content1, Node content2, Calendar date)
      throws ValueFormatException, RepositoryException, PathNotFoundException, IOException,
      UnsupportedEncodingException, UnsupportedRepositoryOperationException
   {
      // Simple
      assertTrue(testRoot.getProperty("booleanT").getBoolean());
      assertFalse(testRoot.getProperty("booleanF").getBoolean());
      assertEquals(date.getTimeInMillis(), testRoot.getProperty("calendar").getDate().getTimeInMillis());
      assertEquals(1234.567, testRoot.getProperty("double").getDouble());
      assertEquals(123456789L, testRoot.getProperty("long").getLong());
      assertEquals("foo", testRoot.getProperty("String").getString());
      InputStream is = testRoot.getProperty("Binary").getStream();
      byte[] buffer = new byte[64];
      int result = is.read(buffer);
      is.close();
      assertEquals("foo", new String(buffer, 0, result, Constants.DEFAULT_ENCODING));
      assertEquals(content1.getUUID(), testRoot.getProperty("Node").getNode().getUUID());
      assertEquals(content1.getName(), testRoot.getProperty("Name").getString());
      assertEquals(content1.getPath(), testRoot.getProperty("Path").getString());

      // Multiple
      Value[] values = testRoot.getProperty("booleanM").getValues();
      assertEquals(2, values.length);
      assertTrue(values[0].getBoolean());
      assertFalse(values[1].getBoolean());
      values = testRoot.getProperty("calendarM").getValues();
      assertEquals(2, values.length);
      assertEquals(date.getTimeInMillis(), values[0].getDate().getTimeInMillis());
      assertEquals(date.getTimeInMillis(), values[1].getDate().getTimeInMillis());
      values = testRoot.getProperty("doubleM").getValues();
      assertEquals(2, values.length);
      assertEquals(1234.567, values[0].getDouble());
      assertEquals(1234.567, values[1].getDouble());
      values = testRoot.getProperty("longM").getValues();
      assertEquals(2, values.length);
      assertEquals(123456789L, values[0].getLong());
      assertEquals(123456789L, values[1].getLong());
      values = testRoot.getProperty("StringM").getValues();
      assertEquals(2, values.length);
      assertEquals("foo", values[0].getString());
      assertEquals("bar", values[1].getString());
      values = testRoot.getProperty("BinaryM").getValues();
      assertEquals(2, values.length);
      is = values[0].getStream();
      buffer = new byte[64];
      result = is.read(buffer);
      is.close();
      assertEquals("foo", new String(buffer, 0, result, Constants.DEFAULT_ENCODING));
      is = values[1].getStream();
      buffer = new byte[64];
      result = is.read(buffer);
      is.close();
      assertEquals("bar", new String(buffer, 0, result, Constants.DEFAULT_ENCODING));
      values = testRoot.getProperty("NodeM").getValues();
      assertEquals(2, values.length);
      assertEquals(content1.getUUID(), values[0].getString());
      assertEquals(content2.getUUID(), values[1].getString());
      values = testRoot.getProperty("NameM").getValues();
      assertEquals(2, values.length);
      assertEquals(content1.getName(), values[0].getString());
      assertEquals(content2.getName(), values[1].getString());
      values = testRoot.getProperty("PathM").getValues();
      assertEquals(2, values.length);
      assertEquals(content1.getPath(), values[0].getString());
      assertEquals(content2.getPath(), values[1].getString());
   }
}
