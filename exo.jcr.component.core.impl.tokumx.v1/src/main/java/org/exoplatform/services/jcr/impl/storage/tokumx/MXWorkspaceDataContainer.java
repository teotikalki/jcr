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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import org.exoplatform.commons.utils.PrivilegedFileHelper;
import org.exoplatform.commons.utils.PrivilegedSystemHelper;
import org.exoplatform.commons.utils.SecurityHelper;
import org.exoplatform.services.jcr.config.RepositoryConfigurationException;
import org.exoplatform.services.jcr.config.RepositoryEntry;
import org.exoplatform.services.jcr.config.SimpleParameterEntry;
import org.exoplatform.services.jcr.config.WorkspaceEntry;
import org.exoplatform.services.jcr.impl.Constants;
import org.exoplatform.services.jcr.impl.core.lock.cacheable.AbstractCacheableLockManager;
import org.exoplatform.services.jcr.impl.dataflow.SpoolConfig;
import org.exoplatform.services.jcr.impl.storage.WorkspaceDataContainerBase;
import org.exoplatform.services.jcr.impl.storage.jdbc.statistics.StatisticsJDBCStorageConnection;
import org.exoplatform.services.jcr.impl.util.io.FileCleanerHolder;
import org.exoplatform.services.jcr.storage.WorkspaceStorageConnection;
import org.exoplatform.services.jcr.storage.value.ValueStoragePluginProvider;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.exoplatform.services.naming.InitialContextInitializer;
import org.picocontainer.Startable;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.Arrays;

import javax.jcr.RepositoryException;
import javax.naming.NamingException;

/**
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class MXWorkspaceDataContainer extends WorkspaceDataContainerBase implements Startable
{

   protected static final Log LOG = ExoLogger.getLogger("exo.jcr.component.core.MXWorkspaceDataContainer");

   /**
    * Indicates if the statistics has to be enabled.
    */
   public static final boolean STATISTICS_ENABLED = Boolean.valueOf(PrivilegedSystemHelper
      .getProperty("JDBCWorkspaceDataContainer.statistics.enabled"));

   static
   {
      if (STATISTICS_ENABLED)
      {
         LOG.info("The statistics of the component MXWorkspaceDataContainer has been enabled");
      }
   }

   /**
    * Workspace configuration.
    */
   private final WorkspaceEntry wsConfig;

   /**
    * Value storage provider
    */
   private final ValueStoragePluginProvider valueStorageProvider;

   /**
    * Spool config
    */
   private final SpoolConfig spoolConfig;

   private static final MongoClient MONGO_CLIENT;
   static
   {
      MongoClient client = null;
      try
      {
         client = new MongoClient();
      }
      catch (UnknownHostException e)
      {
         throw new RuntimeException("Could not connect to mongo db", e);
      }
      MONGO_CLIENT = client;
   }

   /**
    * Constructor with value storage plugins.
    * 
    * @param wsConfig
    *          Workspace configuration
    * @param valueStrorageProvider
    *          External Value Storages provider
    * @throws RepositoryConfigurationException
    *           if Repository configuration is wrong
    * @throws NamingException
    *           if JNDI exception (on DataSource lookup)
    */
   public MXWorkspaceDataContainer(WorkspaceEntry wsConfig, RepositoryEntry repConfig,
      InitialContextInitializer contextInit, ValueStoragePluginProvider valueStorageProvider,
      FileCleanerHolder fileCleanerHolder) throws RepositoryConfigurationException, NamingException,
      RepositoryException, IOException
   {
      this.wsConfig = wsConfig;

      this.valueStorageProvider = valueStorageProvider;

      //      this.containerConfig.batchSize =
      //         wsConfig.getContainer().getParameterInteger(BATCH_SIZE, DEFAULT_BATCHING_DISABLED);

      // ------------- Spool config ------------------
      this.spoolConfig = new SpoolConfig(fileCleanerHolder.getFileCleaner());
      try
      {
         this.spoolConfig.maxBufferSize = wsConfig.getContainer().getParameterInteger(MAXBUFFERSIZE_PROP);
      }
      catch (RepositoryConfigurationException e)
      {
         this.spoolConfig.maxBufferSize = DEF_MAXBUFFERSIZE;
      }

      try
      {
         String sdParam = wsConfig.getContainer().getParameterValue(SWAPDIR_PROP);
         this.spoolConfig.tempDirectory = new File(sdParam);
      }
      catch (RepositoryConfigurationException e1)
      {
         this.spoolConfig.tempDirectory = new File(DEF_SWAPDIR);
      }
      if (!PrivilegedFileHelper.exists(this.spoolConfig.tempDirectory))
      {
         PrivilegedFileHelper.mkdirs(this.spoolConfig.tempDirectory);
      }
      else
      {
         cleanupSwapDirectory();
      }

      LOG.info(getInfo());

      initDatabase();
   }

   /**
    * Deletes all the files from the swap directory 
    */
   private void cleanupSwapDirectory()
   {
      PrivilegedAction<Void> action = new PrivilegedAction<Void>()
      {
         public Void run()
         {
            File[] files = spoolConfig.tempDirectory.listFiles();
            if (files != null && files.length > 0)
            {
               LOG.info("Some files have been found in the swap directory and will be deleted");
               for (int i = 0; i < files.length; i++)
               {
                  File file = files[i];
                  // We don't use the file cleaner in case the deletion failed to ensure
                  // that the file won't be deleted while it is currently re-used
                  file.delete();
               }
            }
            return null;
         }
      };
      SecurityHelper.doPrivilegedAction(action);
   }

   private void initDatabase()
   {
      DB db = getDB();
      try
      {
         db.requestEnsureConnection();
         if (!db.collectionExists(wsConfig.getUniqueName()))
         {
            LOG.debug("The collection '{}' doesn't exist so it will be created", wsConfig.getUniqueName());
            DBCollection collection = db.createCollection(wsConfig.getUniqueName(), new BasicDBObject());
            LOG.debug("The collection '{}' has successfully been created", wsConfig.getUniqueName());
            collection.ensureIndex(new BasicDBObject(MXWorkspaceStorageConnection.ID, 1), new BasicDBObject("unique",
               Boolean.TRUE).append("clustering", Boolean.TRUE));
            collection.ensureIndex(
               new BasicDBObject(MXWorkspaceStorageConnection.PARENT_ID, 1).append(MXWorkspaceStorageConnection.NAME, 1)
                  .append(MXWorkspaceStorageConnection.INDEX, 1).append(MXWorkspaceStorageConnection.IS_NODE, -1)
                  .append(MXWorkspaceStorageConnection.VERSION, -1), new BasicDBObject("unique", Boolean.TRUE));
            collection.ensureIndex(new BasicDBObject(MXWorkspaceStorageConnection.PARENT_ID, 1).append(
               MXWorkspaceStorageConnection.IS_NODE, 1).append(MXWorkspaceStorageConnection.ORDER_NUMBER, 1));
            collection.ensureIndex(new BasicDBObject(MXWorkspaceStorageConnection.VALUES + "."
               + MXWorkspaceStorageConnection.PROPERTY_TYPE_REFERENCE, 1).append(MXWorkspaceStorageConnection.ID, 1),
               new BasicDBObject("sparse", Boolean.TRUE));
            LOG.debug("The indexes of the collection '{}' has successfully created", wsConfig.getUniqueName());
            // A workaround to prevent getting : Cannot transition from not multi key to multi key in multi statement transaction
            // We need to initialize the multi-key index outside a multi-statement transaction
            BasicDBObject node =
               new BasicDBObject(MXWorkspaceStorageConnection.ID, Constants.ROOT_PARENT_UUID)
                  .append(MXWorkspaceStorageConnection.PARENT_ID, Constants.ROOT_PARENT_UUID)
                  .append(MXWorkspaceStorageConnection.NAME, Constants.ROOT_PARENT_UUID)
                  .append(MXWorkspaceStorageConnection.INDEX, 1)
                  .append(MXWorkspaceStorageConnection.IS_NODE, Boolean.TRUE)
                  .append(MXWorkspaceStorageConnection.VERSION, 1)
                  .append(MXWorkspaceStorageConnection.ORDER_NUMBER, 1)
                  .append(
                     MXWorkspaceStorageConnection.VALUES,
                     Arrays.asList(new BasicDBObject(MXWorkspaceStorageConnection.PROPERTY_TYPE_REFERENCE, "foo"),
                        new BasicDBObject(MXWorkspaceStorageConnection.PROPERTY_TYPE_REFERENCE, "foo2")));
            collection.insert(node);
            LOG.debug("The fake node has been added successfully to the collection '{}'", wsConfig.getUniqueName());
            collection.remove(node);
            LOG.debug("The fake node has been removed successfully from the collection '{}'", wsConfig.getUniqueName());
         }
      }
      finally
      {
         db.requestDone();
      }
   }

   private DB getDB()
   {
      return MONGO_CLIENT.getDB("jcr");
   }

   /**
    * {@inheritDoc}
    */
   public boolean isSame(org.exoplatform.services.jcr.storage.WorkspaceDataContainer another)
   {
      return another == this || getUniqueName().equals(another.getUniqueName());
   }

   /**
    * {@inheritDoc}
    */
   public WorkspaceStorageConnection openConnection() throws RepositoryException
   {
      return openConnection(true);
   }

   /**
    * {@inheritDoc}
    */
   public WorkspaceStorageConnection openConnection(boolean readOnly) throws RepositoryException
   {
      WorkspaceStorageConnection con =
         new MXWorkspaceStorageConnection(getDB(), wsConfig.getUniqueName(), readOnly, valueStorageProvider,
            spoolConfig);
      if (STATISTICS_ENABLED)
      {
         con = new StatisticsJDBCStorageConnection(con);
      }
      return con;
   }

   /**
    * {@inheritDoc}
    */
   public WorkspaceStorageConnection reuseConnection(WorkspaceStorageConnection original) throws RepositoryException
   {
      return openConnection(false);
   }

   /**
    * {@inheritDoc}
    */
   public boolean isCheckSNSNewConnection()
   {
      return false;
   }

   /**
    * {@inheritDoc}
    */
   public String getInfo()
   {
      StringBuilder builder = new StringBuilder();

      for (SimpleParameterEntry element : wsConfig.getContainer().getParameters())
      {
         builder.append(element.getName());
         builder.append(":");
         builder.append(element.getValue());
         builder.append(", ");
      }
      builder.append("value storage provider: ");
      builder.append(valueStorageProvider);

      return builder.toString();
   }

   /**
    * {@inheritDoc}
    */
   public String getName()
   {
      return wsConfig.getName();
   }

   /**
    * {@inheritDoc}
    */
   public String getUniqueName()
   {
      return wsConfig.getUniqueName();
   }

   /**
    * {@inheritDoc}
    */
   public String getStorageVersion()
   {
      return "1.0";
   }

   /**
    * {@inheritDoc}
    */
   public void start()
   {
      // Remove lock properties from DB. It is an issue of migration locks from 1.12.x to 1.14.x in case when we use
      // shareable cache. The lock tables will be new but still remaining lock properties in JCR tables.
      boolean deleteLocks =
         "true".equalsIgnoreCase(PrivilegedSystemHelper.getProperty(AbstractCacheableLockManager.LOCKS_FORCE_REMOVE,
            "false"));

      try
      {
         if (deleteLocks)
         {
            boolean failed = true;
            WorkspaceStorageConnection wsc = openConnection(false);
            if (wsc instanceof StatisticsJDBCStorageConnection)
            {
               wsc = ((StatisticsJDBCStorageConnection)wsc).getNestedWorkspaceStorageConnection();
            }
            MXWorkspaceStorageConnection conn = (MXWorkspaceStorageConnection)wsc;
            try
            {
               conn.deleteLockProperties();
               conn.commit();
               failed = false;
            }
            finally
            {
               if (failed)
               {
                  conn.rollback();
               }
            }
         }
      }
      catch (Exception e)
      {
         LOG.error("Can't remove lock properties because of " + e.getMessage(), e);
      }
   }

   /**
    * {@inheritDoc}
    */
   public void stop()
   {
   }

}
