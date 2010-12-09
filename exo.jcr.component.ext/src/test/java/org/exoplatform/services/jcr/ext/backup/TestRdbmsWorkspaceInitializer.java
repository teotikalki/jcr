/*
 * Copyright (C) 2009 eXo Platform SAS.
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
package org.exoplatform.services.jcr.ext.backup;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.exoplatform.services.jcr.config.SimpleParameterEntry;
import org.exoplatform.services.jcr.config.WorkspaceEntry;
import org.exoplatform.services.jcr.config.WorkspaceInitializerEntry;
import org.exoplatform.services.jcr.ext.backup.impl.rdbms.FullBackupJob;
import org.exoplatform.services.jcr.impl.core.RdbmsWorkspaceInitializer;
import org.exoplatform.services.jcr.impl.core.RdbmsWorkspaceInitializerWrapper;
import org.exoplatform.services.jcr.impl.core.SysViewWorkspaceInitializer;
import org.exoplatform.services.jcr.impl.core.query.SystemSearchManager;
import org.exoplatform.services.jcr.impl.core.value.ValueFactoryImpl;
import org.exoplatform.services.jcr.util.IdGenerator;
import org.exoplatform.services.jcr.util.TesterConfigurationHelper;

/**
 * @author <a href="mailto:anatoliy.bazko@gmail.com">Anatoliy Bazko</a>
 * @version $Id: TestFullBackupJob.java 34360 2009-07-22 23:58:59Z tolusha $
 */
public class TestRdbmsWorkspaceInitializer
   extends BaseRDBMSBackupTest
{
   TesterConfigurationHelper helper = TesterConfigurationHelper.getInstence();

   public void testRDBMSInitializerSystemWorkspace() throws Exception
   {
      FullBackupJob job = new FullBackupJob();
      BackupConfig config = new BackupConfig();
      config.setRepository("db1");
      config.setWorkspace("ws");
      config.setBackupDir(new File("target/backup/testJob"));

      Calendar calendar = Calendar.getInstance();

      job.init(repositoryService.getRepository("db1"), "ws", config, calendar);
      job.run();

      URL url = job.getStorageURL();

      for (WorkspaceEntry workspaceEntry : repositoryService.getRepository("db1").getConfiguration()
         .getWorkspaceEntries())
      {
         if (workspaceEntry.getName().equals("ws"))
         {
            String newValueStoragePath = "target/temp/values/" + IdGenerator.generate();
            String newIndexPath = "target/temp/index/" + IdGenerator.generate();

            // set the initializer
            WorkspaceEntry newEntry =
               helper.getNewWs("ws", true, null, newValueStoragePath, newIndexPath, workspaceEntry.getContainer(),
                  workspaceEntry.getContainer().getValueStorages());

            WorkspaceInitializerEntry wiEntry = new WorkspaceInitializerEntry();
            wiEntry.setType(RdbmsWorkspaceInitializer.class.getCanonicalName());

            List<SimpleParameterEntry> wieParams = new ArrayList<SimpleParameterEntry>();
            wieParams.add(new SimpleParameterEntry(SysViewWorkspaceInitializer.RESTORE_PATH_PARAMETER, new File(url
               .getFile()).getParent()));

            wiEntry.setParameters(wieParams);

            newEntry.setInitializer(wiEntry);

            RdbmsWorkspaceInitializerWrapper initializer =
               new RdbmsWorkspaceInitializerWrapper(newEntry,
                  repositoryService.getRepository("db1").getConfiguration(), cacheableDataManager, null, null, null,
                  (ValueFactoryImpl)valueFactory, null);

            initializer.restoreValueFiles();
            assertTrue(new File(newValueStoragePath).list().length > 0);

            initializer.restoreIndexFiles();
            assertTrue(new File(newIndexPath).list().length > 0);
            assertTrue(new File(newIndexPath + "_" + SystemSearchManager.INDEX_DIR_SUFFIX).exists());
            assertTrue(new File(newIndexPath + "_" + SystemSearchManager.INDEX_DIR_SUFFIX).list().length > 0);
         }
      }
   }

   public void testRDBMSInitializer() throws Exception
   {
      FullBackupJob job = new FullBackupJob();
      BackupConfig config = new BackupConfig();
      config.setRepository("db1");
      config.setWorkspace("ws1");
      config.setBackupDir(new File("target/backup/testJob"));

      Calendar calendar = Calendar.getInstance();

      job.init(repositoryService.getRepository("db1"), "ws1", config, calendar);
      job.run();

      URL url = job.getStorageURL();

      for (WorkspaceEntry workspaceEntry : repositoryService.getRepository("db1").getConfiguration()
         .getWorkspaceEntries())
      {
         if (workspaceEntry.getName().equals("ws1"))
         {
            String newValueStoragePath = "target/temp/values/" + IdGenerator.generate();
            String newIndexPath = "target/temp/index/" + IdGenerator.generate();

            // set the initializer
            WorkspaceEntry newEntry =
               helper.getNewWs("ws1", true, null, newValueStoragePath, newIndexPath, workspaceEntry.getContainer(),
                  workspaceEntry.getContainer().getValueStorages());

            WorkspaceInitializerEntry wiEntry = new WorkspaceInitializerEntry();
            wiEntry.setType(RdbmsWorkspaceInitializer.class.getCanonicalName());

            List<SimpleParameterEntry> wieParams = new ArrayList<SimpleParameterEntry>();
            wieParams.add(new SimpleParameterEntry(SysViewWorkspaceInitializer.RESTORE_PATH_PARAMETER, new File(url
               .getFile()).getParent()));

            wiEntry.setParameters(wieParams);

            newEntry.setInitializer(wiEntry);

            RdbmsWorkspaceInitializerWrapper initializer =
               new RdbmsWorkspaceInitializerWrapper(newEntry,
                  repositoryService.getRepository("db1").getConfiguration(), cacheableDataManager, null, null, null,
                  (ValueFactoryImpl)valueFactory, null);

            initializer.restoreValueFiles();
            assertFalse(new File(newValueStoragePath).exists());

            initializer.restoreIndexFiles();
            assertTrue(new File(newIndexPath).list().length > 0);
            assertFalse(new File(newIndexPath + "_" + SystemSearchManager.INDEX_DIR_SUFFIX).exists());
         }
      }
   }

   public void testRDBMSInitializerRestoreTables() throws Exception
   {
      FullBackupJob job = new FullBackupJob();
      BackupConfig config = new BackupConfig();
      config.setRepository("db1");
      config.setWorkspace("ws1");
      config.setBackupDir(new File("target/backup/testJob"));

      Calendar calendar = Calendar.getInstance();

      job.init(repositoryService.getRepository("db1"), "ws1", config, calendar);
      job.run();

      URL url = job.getStorageURL();

      for (WorkspaceEntry workspaceEntry : repositoryService.getRepository("db1").getConfiguration()
         .getWorkspaceEntries())
      {
         if (workspaceEntry.getName().equals("ws1"))
         {
            String newValueStoragePath = "target/temp/values/" + IdGenerator.generate();
            String newIndexPath = "target/temp/index/" + IdGenerator.generate();

            String dsName = helper.getNewDataSource("");
            DataSource ds = (DataSource)new InitialContext().lookup(dsName);

            Connection conn = ds.getConnection();
            Statement st = conn.createStatement();
            st.execute("CREATE TABLE JCR_MITEM(ID VARCHAR(96) NOT NULL,PARENT_ID VARCHAR(96) NOT NULL,NAME VARCHAR(512) NOT NULL,VERSION INTEGER NOT NULL,I_CLASS INTEGER NOT NULL,I_INDEX INTEGER NOT NULL,N_ORDER_NUM INTEGER,P_TYPE INTEGER,P_MULTIVALUED INTEGER,CONSTRAINT JCR_PK_MITEM PRIMARY KEY(ID))");
            conn.commit();

            // set the initializer
            WorkspaceEntry newEntry =
               helper.getNewWs("ws1", true, dsName, newValueStoragePath, newIndexPath, workspaceEntry.getContainer(),
                  workspaceEntry.getContainer().getValueStorages());

            WorkspaceInitializerEntry wiEntry = new WorkspaceInitializerEntry();
            wiEntry.setType(RdbmsWorkspaceInitializer.class.getCanonicalName());

            List<SimpleParameterEntry> wieParams = new ArrayList<SimpleParameterEntry>();
            wieParams.add(new SimpleParameterEntry(SysViewWorkspaceInitializer.RESTORE_PATH_PARAMETER, new File(url
               .getFile()).getParent()));

            wiEntry.setParameters(wieParams);

            newEntry.setInitializer(wiEntry);

            RdbmsWorkspaceInitializerWrapper initializer =
               new RdbmsWorkspaceInitializerWrapper(newEntry,
                  repositoryService.getRepository("db1").getConfiguration(), cacheableDataManager, null, null, null,
                  (ValueFactoryImpl)valueFactory, null);

            initializer.restoreTables(conn, "JCR_MITEM");
         }
      }
   }

}