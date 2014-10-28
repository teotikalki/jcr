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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

import org.exoplatform.commons.utils.SecurityHelper;
import org.exoplatform.services.jcr.config.RepositoryConfigurationException;
import org.exoplatform.services.jcr.datamodel.ValueData;
import org.exoplatform.services.jcr.impl.backup.BackupException;
import org.exoplatform.services.jcr.impl.backup.Backupable;
import org.exoplatform.services.jcr.impl.backup.ComplexDataRestore;
import org.exoplatform.services.jcr.impl.backup.DataRestore;
import org.exoplatform.services.jcr.impl.backup.rdbms.DataRestoreContext;
import org.exoplatform.services.jcr.impl.dataflow.SpoolConfig;
import org.exoplatform.services.jcr.impl.dataflow.ValueDataUtil.ValueDataWrapper;
import org.exoplatform.services.jcr.impl.dataflow.persistent.ChangedSizeHandler;
import org.exoplatform.services.jcr.impl.storage.value.ValueDataNotFoundException;
import org.exoplatform.services.jcr.impl.storage.value.ValueOperation;
import org.exoplatform.services.jcr.impl.storage.value.operations.ValueURLIOHelper;
import org.exoplatform.services.jcr.impl.util.io.DirectoryHelper;
import org.exoplatform.services.jcr.impl.util.io.FileCleaner;
import org.exoplatform.services.jcr.storage.value.ValueIOChannel;
import org.exoplatform.services.jcr.util.tokumx.TokuMXDataRestore;
import org.exoplatform.services.jcr.util.tokumx.Utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class GridFSValueIOChannel implements ValueIOChannel, Backupable
{
   /**
    * Current status of the transaction
    */
   private static final ThreadLocal<Boolean> STATUS = new ThreadLocal<Boolean>();

   /**
    * The GridFS in which it will store the content
    */
   private GridFS gridFS;

   /**
    * The file cleaner
    */
   private final FileCleaner cleaner;

   /**
    * Changes to be saved on commit or rolled back on roll-back.
    */
   private final List<ValueOperation> changes = new ArrayList<ValueOperation>();

   /**
    * The storage 
    */
   private final GridFSValueStorage storage;

   GridFSValueIOChannel(GridFSValueStorage storage)
   {
      this.storage = storage;
      this.gridFS = storage.getGridFS();
      this.cleaner = storage.getCleaner();
   }

   /**
    * Begins the transaction
    */
   private void beginTransaction()
   {
      if (STATUS.get() == null)
      {
         STATUS.set(Boolean.TRUE);
         beginRequest();
         // Starts the transaction
         BasicDBObject command = new BasicDBObject("beginTransaction", Boolean.TRUE).append("isolation", "mvcc");
         gridFS.getDB().command(command);
      }
   }

   /**
    * Begins the transaction
    */
   private void beginRequest()
   {
      gridFS.getDB().requestStart();
      gridFS.getDB().requestEnsureConnection();
   }

   /**
    * {@inheritDoc}
    */
   public ValueDataWrapper read(String propertyId, int orderNumber, int type, SpoolConfig spoolConfig)
      throws IOException
   {
      String key = null;
      try
      {
         beginRequest();
         key = getKey(propertyId, orderNumber);
         return GridFSValueUtil.readValueData(gridFS, key, type, orderNumber, spoolConfig);
      }
      catch (Exception e)
      {
         throw new IOException("Could not read the content of the key " + key, e);
      }
      finally
      {
         endRequest();
      }
   }

   /**
    * {@inheritDoc}
    */
   public void checkValueData(String propertyId, int orderNumber) throws ValueDataNotFoundException, IOException
   {
      String key = null;
      try
      {
         beginRequest();
         key = getKey(propertyId, orderNumber);
         if (!GridFSValueUtil.exists(gridFS, key))
         {
            throw new ValueDataNotFoundException("Value data corresponding to property with [id=" + propertyId
               + ", ordernum=" + orderNumber + "] does not exist.");
         }
      }
      finally
      {
         endRequest();
      }
   }

   /**
    * {@inheritDoc}
    */
   public void repairValueData(String propertyId, int orderNumber) throws IOException
   {
      String key = null;
      try
      {
         beginRequest();
         key = getKey(propertyId, orderNumber);
         GridFSValueUtil.writeValue(gridFS, key, propertyId, new ByteArrayInputStream(new byte[0]));
      }
      catch (Exception e)
      {
         throw new IOException("Could not create an empty content for the property/order"
            + " number corresponding to the key " + key, e);
      }
      finally
      {
         endRequest();
      }

   }

   /**
    * {@inheritDoc}
    */
   public long getValueSize(String propertyId, int orderNumber) throws IOException
   {
      String key = null;
      try
      {
         beginRequest();
         key = getKey(propertyId, orderNumber);
         return GridFSValueUtil.getContentLength(gridFS, key);
      }
      catch (Exception e)
      {
         throw new IOException("Could not get the size of the content for the property/order"
            + " number corresponding to the key " + key, e);
      }
      finally
      {
         endRequest();
      }
   }

   /**
    * {@inheritDoc}
    */
   public long getValueSize(String propertyId) throws IOException
   {
      try
      {
         beginRequest();
         return GridFSValueUtil.getFullContentLength(gridFS, propertyId);
      }
      catch (Exception e)
      {
         throw new IOException("Could not get the size of the the property"
            + " to the key " + propertyId, e);
      }
      finally
      {
         endRequest();
      }
   }

   /**
    * {@inheritDoc}
    */
   public void write(String propertyId, ValueData value, ChangedSizeHandler sizeHandler) throws IOException
   {
      ValueOperation o = new WriteOperation(propertyId, value, sizeHandler);
      o.execute();
      changes.add(o);
   }

   /**
    * {@inheritDoc}
    */
   public void delete(String propertyId) throws IOException
   {
      ValueOperation o = new DeleteOperation(propertyId);
      o.execute();
      changes.add(o);
   }

   /**
    * {@inheritDoc}
    */
   public void close()
   {
      // do nothing
   }

   /**
    * {@inheritDoc}
    */
   public String getStorageId()
   {
      return storage.getId();
   }

   /**
    * {@inheritDoc}
    */
   public void prepare() throws IOException
   {
      beginTransaction();
      for (ValueOperation vo : changes)
         vo.prepare();
   }

   /**
    * {@inheritDoc}
    */
   public void commit() throws IOException
   {
      try
      {
         prepare();
      }
      finally
      {
         twoPhaseCommit();
      }
   }

   /**
    * {@inheritDoc}
    */
   public void twoPhaseCommit() throws IOException
   {
      try
      {
         for (ValueOperation vo : changes)
            vo.twoPhaseCommit();
      }
      finally
      {
         changes.clear();
         endTransaction("commitTransaction");
      }
   }

   /**
    * {@inheritDoc}
    */
   public void rollback() throws IOException
   {
      try
      {
         for (int p = changes.size() - 1; p >= 0; p--)
            changes.get(p).rollback();
      }
      finally
      {
         changes.clear();
         endTransaction("rollbackTransaction");
      }
   }

   /**
    * Ends the transaction
    * @param command the command to execute to end the transaction
    */
   private void endTransaction(String command)
   {
      if (STATUS.get() != null)
      {
         STATUS.remove();
         endRequest();
         gridFS.getDB().command(command);
      }
   }

   /**
    * Ends the request
    */
   private void endRequest()
   {
      gridFS.getDB().requestDone();
   }

   /**
    * Gives the key corresponding to the provided property id and order number
    */
   private String getKey(String propertyId, int orderNumber)
   {
      StringBuilder key = new StringBuilder(64);
      key.append(propertyId);
      key.append(orderNumber);
      return key.toString();
   }

   /**
    * DeleteOperation.
    */
   private class DeleteOperation implements ValueOperation
   {

      private final String propertyId;

      private List<String> keys;

      DeleteOperation(String propertyId)
      {
         this.propertyId = propertyId;
      }

      /**
       * {@inheritDoc}
       */
      public void commit() throws IOException
      {
         // do nothing
      }

      /**
       * {@inheritDoc}
       */
      public void rollback() throws IOException
      {
         if (keys != null)
         {
            for (int i = 0, length = keys.size(); i < length; i++)
            {
               String key = keys.get(i);
               if (key == null)
                  break;
               // As the files could be registered to the file cleaner 
               // to be removed in case of a move that failed
               // or in case of a WriteValue.rollback() that could not
               // remove the file, we need to unregister the files that
               // will be restored thanks to the backup file
               cleaner.removeFile(new GridFSFile(key, gridFS));
            }
         }
      }

      /**
       * {@inheritDoc}
       */
      public void execute() throws IOException
      {
         try
         {
            keys = GridFSValueUtil.getKeys(gridFS, propertyId);
         }
         catch (Exception e)
         {
            throw new IOException("Could not get the keys related to the property " + propertyId, e);
         }
      }

      /**
        * {@inheritDoc}
        */
      public void prepare() throws IOException
      {
         if (keys != null)
         {
            List<String> keysNotYetDeleted = GridFSValueUtil.delete(gridFS, keys);
            if (keysNotYetDeleted != null)
            {
               for (String key : keysNotYetDeleted)
               {
                  cleaner.addFile(new GridFSFile(key, gridFS));
               }
            }
         }
      }

      /**
         * {@inheritDoc}
       */
      public void twoPhaseCommit() throws IOException
      {
         // do nothing
      }
   }

   /**
    * WriteOperation.
    * 
    */
   private class WriteOperation implements ValueOperation
   {

      /**
       * The id of the property
       */
      private final String propertyId;

      /**
       * The key of the value.
       */
      private final String key;

      /**
       * Value.
       */
      private final ValueData value;

      private final ChangedSizeHandler sizeHandler;

      /**
       * WriteOperation constructor.
       * 
       * @param propertyId
       *          String
       * @param value
       *          ValueData
       */
      WriteOperation(String propertyId, ValueData value, ChangedSizeHandler sizeHandler)
      {
         this.propertyId = propertyId;
         this.key = getKey(propertyId, value.getOrderNumber());
         this.value = value;
         this.sizeHandler = sizeHandler;
      }

      /**
       * {@inheritDoc}
       */
      public void commit() throws IOException
      {
         // do nothing
      }

      /**
       * {@inheritDoc}
       */
      public void rollback() throws IOException
      {
         // do nothing
      }

      /**
       * {@inheritDoc}
       */
      public void execute() throws IOException
      {
         // do nothing
      }

      /**
       * {@inheritDoc}
       */
      public void prepare() throws IOException
      {
         // Prevent the file cleaner to remove it
         cleaner.removeFile(new GridFSFile(key, gridFS));

         try
         {
            // write value to GridFS
            long contentSize = GridFSValueUtil.writeValue(gridFS, key, propertyId,  getContent());
            sizeHandler.accumulateNewSize(contentSize);
         }
         catch (Exception e)
         {
            throw new IOException("Could not write the value of the property/order"
               + " number corresponding to the key " + key, e);
         }
      }

      private InputStream getContent() throws IOException
      {
         return ValueURLIOHelper.getContent(storage, value, key, true);
      }

      /**
       * {@inheritDoc}
       */
      public void twoPhaseCommit() throws IOException
      {
         // do nothing
      }
   }

   /**
    * {@inheritDoc}
    */
   public void backup(final File storageDir) throws BackupException
   {
      try
      {
         SecurityHelper.doPrivilegedExceptionAction(new PrivilegedExceptionAction<Void>()
         {
            public Void run() throws RepositoryConfigurationException, IOException, BackupException
            {
               final File parentFolder = new File(storageDir, "values-" + getStorageId());
               parentFolder.mkdirs();
               final File filesContent = new File(parentFolder, "files.dat");
               final File chunksContent = new File(parentFolder, "chuncks.dat");
               DB database = storage.getNewDBInstance();
               try
               {
                  Utils.backup(database, gridFS.getFilesCollection().getName(), filesContent);
                  Utils.backup(database, gridFS.getChunkCollection().getName(), chunksContent);
               }
               finally
               {
                  // A new Mongo has been created to acces to the database so we need
                  // to close it
                  database.getMongo().close();
               }
               // Zip the file
               DirectoryHelper.compressDirectory(parentFolder, new File(storageDir, "values-" + getStorageId() + ".zip"));
               // Remove the files
               filesContent.delete();
               chunksContent.delete();
               parentFolder.delete();
               return null;
            }
         });
      }
      catch (PrivilegedActionException e)
      {
         if (e.getCause() instanceof BackupException)
         {
            throw (BackupException)e.getCause();
         }
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
      Utils.clean(gridFS.getDB(), gridFS.getFilesCollection().getName());
      Utils.clean(gridFS.getDB(), gridFS.getChunkCollection().getName());
      storage.resetGridFS();
      this.gridFS = storage.getGridFS();
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
         restorers.add(new FilesTokuMXDataRestore(storageDir));
         restorers.add(new ChunksTokuMXDataRestore(storageDir));
         return new ComplexDataRestore(restorers);
      }
      catch (Exception e)
      {
         throw new BackupException(e);
      }
   }

   private class FilesTokuMXDataRestore extends TokuMXDataRestore
   {
      FilesTokuMXDataRestore(File storageDir)
      {
         super(gridFS.getDB(), gridFS.getFilesCollection().getName(), "values-" + getStorageId() + ".zip", "files.dat",
            storageDir);
      }

      /**
       * {@inheritDoc}
       */
      public void clean() throws BackupException
      {
      }
   }

   private class ChunksTokuMXDataRestore extends TokuMXDataRestore
   {
      ChunksTokuMXDataRestore(File storageDir)
      {
         super(gridFS.getDB(), gridFS.getChunkCollection().getName(), "values-" + getStorageId() + ".zip", "chuncks.dat",
            storageDir);
      }

      /**
       * {@inheritDoc}
       */
      public void clean() throws BackupException
      {
         storage.resetGridFS();
         gridFS = storage.getGridFS();
      }

      /**
       * {@inheritDoc}
       */
      protected void postRestore(DBCollection collection)
      {
         storage.resetGridFS();
         gridFS = storage.getGridFS();
      }
   }
}
