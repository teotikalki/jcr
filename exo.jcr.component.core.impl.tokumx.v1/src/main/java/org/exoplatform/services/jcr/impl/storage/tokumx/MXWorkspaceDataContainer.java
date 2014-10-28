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
import com.mongodb.MongoClientURI;

import org.exoplatform.commons.utils.PrivilegedFileHelper;
import org.exoplatform.commons.utils.PrivilegedSystemHelper;
import org.exoplatform.commons.utils.SecurityHelper;
import org.exoplatform.services.jcr.config.RepositoryConfigurationException;
import org.exoplatform.services.jcr.config.RepositoryEntry;
import org.exoplatform.services.jcr.config.SimpleParameterEntry;
import org.exoplatform.services.jcr.config.ValueStorageEntry;
import org.exoplatform.services.jcr.config.WorkspaceEntry;
import org.exoplatform.services.jcr.impl.Constants;
import org.exoplatform.services.jcr.impl.backup.BackupException;
import org.exoplatform.services.jcr.impl.backup.Backupable;
import org.exoplatform.services.jcr.impl.backup.ComplexDataRestore;
import org.exoplatform.services.jcr.impl.backup.DataRestore;
import org.exoplatform.services.jcr.impl.backup.rdbms.DataRestoreContext;
import org.exoplatform.services.jcr.impl.backup.rdbms.DirectoryRestore;
import org.exoplatform.services.jcr.impl.core.lock.cacheable.AbstractCacheableLockManager;
import org.exoplatform.services.jcr.impl.core.query.NodeDataIndexingIterator;
import org.exoplatform.services.jcr.impl.core.query.Reindexable;
import org.exoplatform.services.jcr.impl.dataflow.SpoolConfig;
import org.exoplatform.services.jcr.impl.storage.WorkspaceDataContainerBase;
import org.exoplatform.services.jcr.impl.storage.statistics.StatisticsWorkspaceStorageConnection;
import org.exoplatform.services.jcr.impl.storage.tokumx.indexing.MXNodeDataIndexingIterator;
import org.exoplatform.services.jcr.impl.storage.value.ValueStorageNotFoundException;
import org.exoplatform.services.jcr.impl.storage.value.fs.FileValueStorage;
import org.exoplatform.services.jcr.impl.util.io.DirectoryHelper;
import org.exoplatform.services.jcr.impl.util.io.FileCleanerHolder;
import org.exoplatform.services.jcr.storage.WorkspaceStorageConnection;
import org.exoplatform.services.jcr.storage.value.ValueIOChannel;
import org.exoplatform.services.jcr.storage.value.ValueStoragePluginProvider;
import org.exoplatform.services.jcr.util.tokumx.TokuMXDataRestore;
import org.exoplatform.services.jcr.util.tokumx.Utils;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.exoplatform.services.naming.InitialContextInitializer;
import org.picocontainer.Startable;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.jcr.RepositoryException;
import javax.naming.NamingException;

/**
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class MXWorkspaceDataContainer extends WorkspaceDataContainerBase implements Startable, Reindexable, Backupable
{

   private static final Log LOG = ExoLogger.getLogger("exo.jcr.component.core.impl.tokumx.v1.MXWorkspaceDataContainer");

   /**
    * Indicates if the statistics has to be enabled.
    */
   public static final boolean STATISTICS_ENABLED = Boolean.valueOf(PrivilegedSystemHelper
      .getProperty("WorkspaceDataContainer.statistics.enabled"));

   static
   {
      if (STATISTICS_ENABLED)
      {
         StatisticsWorkspaceStorageConnection.registerStatistics();
         LOG.info("The statistics of the component MXWorkspaceDataContainer has been enabled");
      }
   }

   /**
    * Suffix for content zip file.
    */
   public static final String CONTENT_ZIP_FILE = "dump.zip";

   /**
    * The name of the content file.
    */
   public static final String CONTENT_FILE = "content.dat";

   /**
    * Suffix used in collection names
    */
   public final static String COLLECTION_NAME_SUFFIX = "collection-name-suffix";

   /**
    * Indicates whether the auto commit mode must be enabled
    */
   public final static String AUTO_COMMIT = "auto-commit";

   /**
    * Use sequence for order number
    */
   public final static String USE_SEQUENCE_FOR_ORDER_NUMBER = "use-sequence-for-order-number";

   /**
    * The mongo connection URI
    */
   public final static String CONNECTION_URI = "connection-uri";

   /**
    * Batch size value parameter name.
    */
   public final static String BATCH_SIZE = "batch-size";

   public final static int DEFAULT_BATCHING_DISABLED = -1;

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

   /**
    * The name of the collection
    */
   private final String collectionName;

   /**
    * Indicates whether the auto commit mode is enabled
    * The auto commit mode is disabled by default
    * Enabling the auto commit mode allows to use MongoDB directly or the sharding mode
    */
   private final boolean autoCommit;

   /**
    * Indicates whether a sequence should be used to get the last order number
    */
   private final boolean useSequenceForOrderNumber;

   /**
    * Batch size.
    */
   public final int batchSize;

   /**
    * The target database
    */
   private final DB database;

   /**
    * The connection pool used to access to the database
    */
   private final MongoClient client;

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

      this.batchSize = wsConfig.getContainer().getParameterInteger(BATCH_SIZE, DEFAULT_BATCHING_DISABLED);

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
      String collectionNameSuffix =
         wsConfig.getContainer().getParameterValue(COLLECTION_NAME_SUFFIX, wsConfig.getUniqueName());
      this.collectionName = Utils.getCollectionName("jcr", collectionNameSuffix);
      this.autoCommit = wsConfig.getContainer().getParameterBoolean(AUTO_COMMIT, Boolean.FALSE);
      this.useSequenceForOrderNumber =
         wsConfig.getContainer().getParameterBoolean(USE_SEQUENCE_FOR_ORDER_NUMBER, Boolean.TRUE);

      MongoClientURI uri = new MongoClientURI(wsConfig.getContainer().getParameterValue(CONNECTION_URI));
      String databaseName = uri.getDatabase() == null || uri.getDatabase().isEmpty() ? "jcr" : uri.getDatabase();
      this.client = new MongoClient(uri);
      this.database = client.getDB(databaseName);

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
      db.requestStart();
      try
      {
         db.requestEnsureConnection();
         if (!db.collectionExists(collectionName))
         {
            DBCollection collection = createCollection(db);
            addIndexes(collection);
            initCollection(collection);
         }
      }
      finally
      {
         db.requestDone();
      }
   }

   /**
    * Initializes the collection
    */
   private void initCollection(DBCollection collection)
   {
      if (!autoCommit)
      {
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
         LOG.debug("The fake node has been added successfully to the collection '{}'", collectionName);
         collection.remove(node);
         LOG.debug("The fake node has been removed successfully from the collection '{}'", collectionName);
      }
   }

   /**
    * Creates the collection and returns it
    */
   private DBCollection createCollection(DB db)
   {
      LOG.debug("The collection '{}' doesn't exist so it will be created", collectionName);
      DBCollection collection = db.createCollection(collectionName, new BasicDBObject());
      return collection;
   }

   /**
    * Adds all the indexes to the collection
    */
   private void addIndexes(DBCollection collection)
   {
      LOG.debug("The collection '{}' has successfully been created", collectionName);
      collection.ensureIndex(
         new BasicDBObject(MXWorkspaceStorageConnection.PARENT_ID, 1)
            .append(MXWorkspaceStorageConnection.NAME, 1).append(MXWorkspaceStorageConnection.INDEX, 1)
            .append(MXWorkspaceStorageConnection.IS_NODE, -1).append(MXWorkspaceStorageConnection.VERSION, -1),
         new BasicDBObject("unique", Boolean.TRUE));
      collection.ensureIndex(new BasicDBObject(MXWorkspaceStorageConnection.PARENT_ID, 1).append(
         MXWorkspaceStorageConnection.IS_NODE, 1).append(MXWorkspaceStorageConnection.ORDER_NUMBER, 1));
      collection.ensureIndex(new BasicDBObject(MXWorkspaceStorageConnection.IS_NODE, 1).append(
         MXWorkspaceStorageConnection.ID, 1));
      collection.ensureIndex(new BasicDBObject(MXWorkspaceStorageConnection.VALUES + "."
         + MXWorkspaceStorageConnection.PROPERTY_TYPE_REFERENCE, 1).append(MXWorkspaceStorageConnection.ID, 1),
         new BasicDBObject("sparse", Boolean.TRUE));
      LOG.debug("The indexes of the collection '{}' has successfully created", collectionName);
   }

   /**
    * Gives the target database
    * @return
    */
   private DB getDB()
   {
      return database;
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
         new MXWorkspaceStorageConnection(getDB(), collectionName, readOnly, autoCommit, useSequenceForOrderNumber,
            batchSize, valueStorageProvider, spoolConfig);
      if (STATISTICS_ENABLED)
      {
         con = new StatisticsWorkspaceStorageConnection(con);
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
            if (wsc instanceof StatisticsWorkspaceStorageConnection)
            {
               wsc = ((StatisticsWorkspaceStorageConnection)wsc).getNestedWorkspaceStorageConnection();
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
      if (client != null)
         client.close();
   }

   /**
    * {@inheritDoc}
    */
   public NodeDataIndexingIterator getNodeDataIndexingIterator(int pageSize) throws RepositoryException
   {
      return new MXNodeDataIndexingIterator(this, pageSize);
   }

   /**
    * {@inheritDoc}
    */
   public boolean isReindexingSupported()
   {
      return true;
   }

   /**
    * {@inheritDoc}
    */
   public Long getNodesCount() throws RepositoryException
   {
      WorkspaceStorageConnection conn = openConnection(true);
      try
      {
         return conn.getNodesCount();
      }
      finally
      {
         conn.close();
      }
   }

   /**
    * {@inheritDoc}
    */
   public void backup(final File storageDir) throws BackupException
   {
      final File file = new File(storageDir, CONTENT_FILE);
      Utils.backup(getDB(), collectionName, file);
      try
      {
         SecurityHelper.doPrivilegedExceptionAction(new PrivilegedExceptionAction<Void>()
         {
            public Void run() throws RepositoryConfigurationException, IOException
            {
               // Zip the file
               DirectoryHelper.compressDirectory(file, new File(storageDir, CONTENT_ZIP_FILE));
               // Remove the file
               if (!file.delete())
                  spoolConfig.fileCleaner.addFile(file);
               // backup value storage
               if (wsConfig.getContainer().getValueStorages() != null)
               {
                  for (ValueStorageEntry valueStorage : wsConfig.getContainer().getValueStorages())
                  {
                     ValueIOChannel channel = null;
                     try
                     {
                        channel = valueStorageProvider.getChannel(valueStorage.getId());
                        if (channel instanceof Backupable)
                        {
                           ((Backupable)channel).backup(storageDir);
                           continue;
                        }
                     }
                     catch (ValueStorageNotFoundException e)
                     {
                        // ignore me
                     }
                     catch(BackupException e)
                     {
                        throw new IOException("Could not backup the value storage " + valueStorage.getId(), e);
                     }
                     finally
                     {
                        if (channel != null)
                           channel.close();
                     }
                     File srcDir = new File(valueStorage.getParameterValue(FileValueStorage.PATH));

                     if (!srcDir.exists())
                     {
                        throw new IOException("Can't backup value storage. Directory " + srcDir.getName()
                           + " doesn't exists");
                     }
                     else
                     {
                        File zipFile = new File(storageDir, "values-" + valueStorage.getId() + ".zip");
                        DirectoryHelper.compressDirectory(srcDir, zipFile);
                     }
                  }
               }
               return null;
            }
         });
      }
      catch (PrivilegedActionException e)
      {
         throw new BackupException(e);
      }
      catch (Exception e)
      {
         throw new BackupException(e);
      }
   }

   /**
    * {@inheritDoc}
    */
   public void clean() throws BackupException
   {
      LOG.info("Start to clean the data of the workspace '" + wsConfig.getName() + "'");
      Utils.clean(getDB(), collectionName);
      initDatabase();
      cleanVS();
   }

   /**
    * @throws BackupException
    */
   private void cleanVS() throws BackupException
   {
      try
      {
         if (wsConfig.getContainer().getValueStorages() != null)
         {
            SecurityHelper.doPrivilegedExceptionAction(new PrivilegedExceptionAction<Void>()
            {
               public Void run() throws IOException, RepositoryConfigurationException
               {
                  for (ValueStorageEntry valueStorage : wsConfig.getContainer().getValueStorages())
                  {
                     ValueIOChannel channel = null;
                     try
                     {
                        channel = valueStorageProvider.getChannel(valueStorage.getId());
                        if (channel instanceof Backupable)
                        {
                           ((Backupable)channel).clean();
                           continue;
                        }
                     }
                     catch (ValueStorageNotFoundException e)
                     {
                        // ignore me
                     }
                     catch(BackupException e)
                     {
                        throw new IOException("Could not clean the value storage " + valueStorage.getId(), e);
                     }
                     finally
                     {
                        if (channel != null)
                           channel.close();
                     }
                     File valueStorageDir = new File(valueStorage.getParameterValue(FileValueStorage.PATH));
                     if (valueStorageDir.exists())
                     {
                        DirectoryHelper.removeDirectory(valueStorageDir);
                     }
                  }

                  return null;
               }
            });
         }
      }
      catch (PrivilegedActionException e)
      {
         throw new BackupException(e);
      }
   }

   /**
    * {@inheritDoc}
    */
   public DataRestore getDataRestorer(DataRestoreContext context) throws BackupException
   {
      try
      {
         File storageDir = (File)context.getObject(DataRestoreContext.STORAGE_DIR);
         List<DataRestore> restorers = new ArrayList<DataRestore>();
         restorers.add(new MXDataRestore(database, collectionName, storageDir));
         if (wsConfig.getContainer().getValueStorages() != null)
         {
            for (ValueStorageEntry valueStorage : wsConfig.getContainer().getValueStorages())
            {
               ValueIOChannel channel = null;
               try
               {
                  channel = valueStorageProvider.getChannel(valueStorage.getId());
                  if (channel instanceof Backupable)
                  {
                     restorers.add(((Backupable)channel).getDataRestorer(context));
                  }
               }
               catch (ValueStorageNotFoundException e)
               {
                  // ignore me
               }
               catch(BackupException e)
               {
                  throw new IOException("Could not clean the value storage " + valueStorage.getId(), e);
               }
            }

            List<File> dataDirsList = initDataDirs();
            List<File> backupDirsList = initBackupDirs(storageDir);
            if (!dataDirsList.isEmpty())
               restorers.add(new DirectoryRestore(dataDirsList, backupDirsList));
         }

         return new ComplexDataRestore(restorers);
      }
      catch (Exception e)
      {
         throw new BackupException(e);
      }
   }

   private List<File> initBackupDirs(File storageDir) throws RepositoryConfigurationException
   {
      List<File> backupDirsList = new ArrayList<File>();

      for (ValueStorageEntry valueStorage : wsConfig.getContainer().getValueStorages())
      {
         String path = valueStorage.getParameterValue(FileValueStorage.PATH, null);
         if (path == null)
            continue;
         File zipFile = new File(storageDir, "values-" + valueStorage.getId() + ".zip");
         if (PrivilegedFileHelper.exists(zipFile))
         {
            backupDirsList.add(zipFile);
         }
         else
         {
            // try to check if we have deal with old backup format
            zipFile = new File(storageDir, "values/" + valueStorage.getId());
            if (PrivilegedFileHelper.exists(zipFile))
            {
               backupDirsList.add(zipFile);
            }
            else
            {
               throw new RepositoryConfigurationException("There is no backup data for value storage with id "
                  + valueStorage.getId());
            }
         }
      }

      return backupDirsList;
   }

   private List<File> initDataDirs() throws RepositoryConfigurationException
   {
      List<File> dataDirsList = new ArrayList<File>();

      for (ValueStorageEntry valueStorage : wsConfig.getContainer().getValueStorages())
      {
         String path = valueStorage.getParameterValue(FileValueStorage.PATH, null);
         if (path == null)
            continue;
         File dataDir = new File(path);
         dataDirsList.add(dataDir);
      }

      return dataDirsList;
   }

   private class MXDataRestore extends TokuMXDataRestore
   {
      public MXDataRestore(DB database, String collectionName, File storageDir)
      {
         super(database, collectionName, CONTENT_ZIP_FILE, CONTENT_FILE, storageDir);
      }

      /**
       * {@inheritDoc}
       */
      public void clean() throws BackupException
      {
         super.clean();
         LOG.info("Drop the VS of the workspace '" + wsConfig.getName() + "'");
         cleanVS();
      }

      /**
       * Executes an action after the restore
       */
      protected void postRestore(DBCollection collection)
      {
         addIndexes(collection);
         initCollection(collection);
      }
   }
}
