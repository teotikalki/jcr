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
package org.exoplatform.services.jcr.util.tokumx;

import org.exoplatform.services.jcr.config.CacheEntry;
import org.exoplatform.services.jcr.config.ContainerEntry;
import org.exoplatform.services.jcr.config.LockManagerEntry;
import org.exoplatform.services.jcr.config.QueryHandlerEntry;
import org.exoplatform.services.jcr.config.RepositoryConfigurationException;
import org.exoplatform.services.jcr.config.SimpleParameterEntry;
import org.exoplatform.services.jcr.config.ValueStorageEntry;
import org.exoplatform.services.jcr.config.ValueStorageFilterEntry;
import org.exoplatform.services.jcr.config.WorkspaceEntry;
import org.exoplatform.services.jcr.core.ManageableRepository;
import org.exoplatform.services.jcr.impl.storage.jdbc.JDBCDataContainerConfig.DatabaseStructureType;
import org.exoplatform.services.jcr.impl.storage.jdbc.JDBCWorkspaceDataContainer;
import org.exoplatform.services.jcr.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;

import javax.jcr.RepositoryException;

/**
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class TesterConfigurationHelper extends org.exoplatform.services.jcr.util.TesterConfigurationHelper
{

   public WorkspaceEntry createWorkspaceEntry(DatabaseStructureType dbStructureType, String dsName,
      List<String> valueStorageIds, boolean cacheEnabled, boolean cacheShared) throws Exception
   {

      if ("not-existed-ds".equals(dsName))
      {
         // This is needed to avoid a failure in TestWorkspaceManagement.testAddWorkspaceWithNewDS
         throw new Exception("Not existing data source");
      }
      String id = IdGenerator.generate();
      String wsName = "ws-" + id;

      // container entry
      List<SimpleParameterEntry> params = new ArrayList<SimpleParameterEntry>();
      params.add(new SimpleParameterEntry(JDBCWorkspaceDataContainer.DB_STRUCTURE_TYPE, dbStructureType.toString()));
      params.add(new SimpleParameterEntry("collection-name-suffix", wsName));
      params.add(new SimpleParameterEntry("auto-commit", System.getProperty("auto-commit", "true")));
      params.add(new SimpleParameterEntry("use-sequence-for-order-number", System.getProperty("use-sequence", "true")));
      params
         .add(new SimpleParameterEntry("connection-uri", System.getProperty("connection-uri", "mongodb://127.0.0.1")));
      params.add(new SimpleParameterEntry("max-buffer-size", "204800"));
      params.add(new SimpleParameterEntry("swap-directory", "target/temp/swap/" + wsName));

      ContainerEntry containerEntry =
         new ContainerEntry("org.exoplatform.services.jcr.impl.storage.tokumx.MXWorkspaceDataContainer", params);
      containerEntry.setParameters(params);

      // value storage
      List<ValueStorageEntry> list = new ArrayList<ValueStorageEntry>();
      if (valueStorageIds != null)
      {
         for (String vsId : valueStorageIds)
         {
            List<ValueStorageFilterEntry> vsparams = new ArrayList<ValueStorageFilterEntry>();
            ValueStorageFilterEntry filterEntry = new ValueStorageFilterEntry();
            filterEntry.setPropertyType("Binary");
            vsparams.add(filterEntry);

            List<SimpleParameterEntry> spe = new ArrayList<SimpleParameterEntry>();
            spe.add(new SimpleParameterEntry("path", "target/temp/values/" + wsName + "-" + vsId));

            ValueStorageEntry valueStorageEntry =
               new ValueStorageEntry("org.exoplatform.services.jcr.impl.storage.value.fs.SimpleFileValueStorage", spe);

            valueStorageEntry.setId(vsId);
            valueStorageEntry.setFilters(vsparams);

            // containerEntry.setValueStorages();
            containerEntry.setParameters(params);
            list.add(valueStorageEntry);
         }
      }

      containerEntry.setValueStorages(list);

      // Indexer
      params = new ArrayList<SimpleParameterEntry>();
      params.add(new SimpleParameterEntry("index-dir", "target/temp/index/" + wsName));
      QueryHandlerEntry qEntry =
         new QueryHandlerEntry("org.exoplatform.services.jcr.impl.core.query.lucene.SearchIndex", params);

      // Cache
      CacheEntry cacheEntry = null;

      try
      {
         Class
            .forName("org.exoplatform.services.jcr.impl.dataflow.persistent.infinispan.ISPNCacheWorkspaceStorageCache");

         List<SimpleParameterEntry> cacheParams = new ArrayList<SimpleParameterEntry>();
         cacheParams.add(new SimpleParameterEntry("infinispan-configuration",
            "conf/standalone/test-infinispan-config.xml"));
         cacheEntry = new CacheEntry(cacheParams);
         cacheEntry.setEnabled(cacheEnabled);
         cacheEntry
            .setType("org.exoplatform.services.jcr.impl.dataflow.persistent.infinispan.ISPNCacheWorkspaceStorageCache");
      }
      catch (ClassNotFoundException e)
      {
         throw e;
      }

      LockManagerEntry lockManagerEntry = new LockManagerEntry();
      lockManagerEntry.putParameterValue("time-out", "15m");

      // ISPN Lock
      try
      {
         Class.forName("org.exoplatform.services.jcr.impl.core.lock.tokumx.ISPNCacheableLockManagerImpl");

         lockManagerEntry.setType("org.exoplatform.services.jcr.impl.core.lock.tokumx.ISPNCacheableLockManagerImpl");
         lockManagerEntry.putParameterValue("infinispan-configuration", "conf/standalone/test-infinispan-lock.xml");
         lockManagerEntry.putParameterValue("infinispan-cl-cache.mongodb.connection.uri",
            System.getProperty("connection-uri", "mongodb://127.0.0.1"));
         lockManagerEntry.putParameterValue("infinispan-cl-cache.mongodb.auto.commit",
            System.getProperty("auto-commit", "true"));
         lockManagerEntry.putParameterValue("infinispan-cl-cache.mongodb.collection.name", wsName);
      }
      catch (ClassNotFoundException e)
      {
         throw e;
      }

      WorkspaceEntry workspaceEntry = new WorkspaceEntry();
      workspaceEntry.setContainer(containerEntry);
      workspaceEntry.setCache(cacheEntry);
      workspaceEntry.setQueryHandler(qEntry);
      workspaceEntry.setLockManager(lockManagerEntry);
      workspaceEntry.setName(wsName);

      return workspaceEntry;
   }

   /**
    * Add new workspace to repository.
    */
   public void addWorkspace(ManageableRepository repository, WorkspaceEntry workspaceEntry)
      throws RepositoryConfigurationException, RepositoryException
   {
      super.addWorkspace(repository, workspaceEntry);
      String dbStructure = null;
      for (WorkspaceEntry entry : repository.getConfiguration().getWorkspaceEntries())
      {
         String dbStr = entry.getContainer().getParameterValue(JDBCWorkspaceDataContainer.DB_STRUCTURE_TYPE);
         if (dbStructure == null)
         {
            dbStructure = dbStr;
         }
         else if (!dbStructure.equals(dbStr))
         {
            // This is required to avoid a failure in TestWorkspaceManagement.testMixMultiAndSingleDbWs
            throw new RepositoryException("Cannot mix db structure");
         }
      }
   }

   public void setSourceName(WorkspaceEntry ws, String newDatasourceName)
   {
      if (ws.getContainer().hasParameter("collection-name-suffix"))
      {
         ws.getContainer().addParameter(new SimpleParameterEntry("collection-name-suffix", newDatasourceName));
      }

      if (ws.getLockManager().hasParameter("infinispan-cl-cache.mongodb.collection.name"))
      {
         ws.getLockManager().addParameter(
            new SimpleParameterEntry("infinispan-cl-cache.mongodb.collection.name", newDatasourceName));
      }
   }

}
