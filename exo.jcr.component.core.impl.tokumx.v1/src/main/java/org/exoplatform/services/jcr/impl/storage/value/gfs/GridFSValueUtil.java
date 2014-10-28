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
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import org.exoplatform.services.jcr.datamodel.ValueData;
import org.exoplatform.services.jcr.impl.dataflow.SpoolConfig;
import org.exoplatform.services.jcr.impl.dataflow.ValueDataUtil;
import org.exoplatform.services.jcr.impl.dataflow.ValueDataUtil.ValueDataWrapper;
import org.exoplatform.services.jcr.impl.dataflow.persistent.CleanableFilePersistedValueData;
import org.exoplatform.services.jcr.impl.storage.value.fs.operations.ValueFileIOHelper;
import org.exoplatform.services.jcr.impl.util.io.SwapFile;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An utility class that will propose the most common operations on the GridFS of MongoDB
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class GridFSValueUtil
{
   private static final Log LOG = ExoLogger.getLogger("exo.jcr.component.core.impl.tokumx.v1.GridFSValueUtil");

   private static final AtomicLong SEQUENCE = new AtomicLong();
   public static final String GROUP_KEY = "group_key";

   private GridFSValueUtil() 
   {
   }

   /**
    * Deletes a given key for a given GridFS
    * @param gfs the GridFS in which we do the deletion
    * @param key the key to delete
    * @return <code>true</code> if it could be deleted, <code>false</code> otherwise
    */
   public static boolean delete(GridFS gfs, String key)
   {
      try
      {
         gfs.remove(key);
         return true;
      }
      catch (Exception e)
      {
         LOG.warn("Could not delete {} due to {} ", key, e.getMessage());
         LOG.debug(e);
      }
      return false;
   }

   /**
    * Deletes all the provided key within the context of the given
    * GridFS
    * @param gfs the GridFS in which we do the deletion
    * @param keys the keys to delete
    * @return the {@link List} of keys that could not be deleted
    */
   public static List<String> delete(GridFS gfs, List<String> keys)
   {
      List<String> result = null;
      for (int i = 0, length = keys.size(); i <length; i++)
      {
         String key = keys.get(i);
         try
         {
            gfs.remove(key);
         }
         catch (MongoException e)
         {
            if (result == null)
               result = new ArrayList<String>();
            result.add(key);
         }
      }
      return result;
   }

   /**
    * Indicates whether a given key exists within the context of the given
    * GridFS
    * @param gfs the GridFS in which we do the lookup
    * @param key the key to check
    * @return <code>true</code> if it exists, <code>false</code> otherwise
    */
   public static boolean exists(GridFS gfs, String key)
   {
      return gfs.findOne(key) != null;
   }

   /**
    * Gives the list of all keys that match with given <code>groupKey<code>
    * @param gfs the GridFS in which we do the retrieval of the keys
    * @param groupKey the key of the group that we want to query
    * @return a {@link List} of keys that match with the given group key
    */
   public static List<String> getKeys(GridFS gfs, String groupKey)
   {
      BasicDBObject query = new BasicDBObject(GROUP_KEY, groupKey);
      String fieldName = gfs.getFileNameField();
      DBCursor files = gfs.getFileList(query);
      List<String> result = new ArrayList<String>();
      try 
      {
         while (files.hasNext())
         {
            DBObject o = files.next();
            result.add((String)o.get(fieldName));
         }
      }
      finally 
      {
         files.close();
      }

      return result;
   }

   /**
    * Reads the content of a provided key within the context of the given
    * GridFS
    * @param gfs the GridFS in which we do the get
    * @param key the key for which we extract the content
    * @param type the type of the corresponding property
    * @param orderNumber the order number of the corresponding property
    * @param spoolConfig the configuration for spooling
    * @return the corresponding {@link ValueData}
    * @throws IOException if an error occurs
    */
   public static ValueDataWrapper readValueData(GridFS gfs, String key, int type, int orderNumber, SpoolConfig spoolConfig)
      throws IOException
   {
      GridFSDBFile file = gfs.findOne(key);
      if (file == null)
         throw new FileNotFoundException("Could not find the key " + key);
      ValueDataWrapper vdDataWrapper = new ValueDataWrapper();
      long fileSize = file.getLength();
      vdDataWrapper.size = fileSize;

      if (fileSize > spoolConfig.maxBufferSize)
      {
         SwapFile swapFile =
            SwapFile.get(spoolConfig.tempDirectory, key.replace('/', '_') + "." + System.currentTimeMillis() + "_"
               + SEQUENCE.incrementAndGet(), spoolConfig.fileCleaner);
         // spool GridFS Value content into swap file
         try
         {
            file.writeTo(swapFile);
         }
         finally
         {
            swapFile.spoolDone();
         }

         vdDataWrapper.value = new CleanableFilePersistedValueData(orderNumber, swapFile, spoolConfig);
      }
      else
      {
         InputStream is = file.getInputStream();
         try
         {
            byte[] data = new byte[(int)fileSize];
            byte[] buff =
               new byte[ValueFileIOHelper.IOBUFFER_SIZE > fileSize ? ValueFileIOHelper.IOBUFFER_SIZE : (int)fileSize];

            int rpos = 0;
            int read;

            while ((read = is.read(buff)) >= 0)
            {
               System.arraycopy(buff, 0, data, rpos, read);
               rpos += read;
            }

            vdDataWrapper.value = ValueDataUtil.createValueData(type, orderNumber, data);
         }
         finally
         {
            is.close();
         }
      }
      return vdDataWrapper;
   }

   /**
    * Stores a given content for the provided key within the context of the given
    * GridFS
    * @param gfs the GridFS in which we do the put
    * @param key the key to add
    * @param groupKey the key allowing to group all the values
    * @param content the content of the key to add
    * @throws IOException If an error prevents to get the size of the content
    */
   public static long writeValue(GridFS gfs, String key, String groupKey, InputStream content) throws IOException
   {
      GridFSInputFile file = gfs.createFile(content, key, true);
      file.put(GROUP_KEY, groupKey);
      file.save();
      return file.getLength();
   }

   /**
    * Finds the length of the file corresponding to the given key
    * @param gfs the GridFS from which we get the corresponding file
    * @param key the key corresponding to the file to find
    * @return the length of the corresponding file
    */
   public static long getContentLength(GridFS gfs, String key)
   {
      GridFSDBFile file = gfs.findOne(key);
      if (file == null)
         return -1;
      return file.getLength();
   }

   /**
    * Finds the length of the files corresponding to the given key
    * @param gfs the GridFS from which we get the corresponding files
    * @param groupKey the key of the group corresponding to the files to find
    * @return the length of the corresponding files
    */
   public static long getFullContentLength(GridFS gfs, String groupKey)
   {
      BasicDBObject query = new BasicDBObject(GROUP_KEY, groupKey);
      List<GridFSDBFile> files = gfs.find(query);
      long size = -1;
      for (int i = 0, length = files.size(); i < length; i++)
      {
         long l = files.get(i).getLength();
         if (l > 0)
         {
            if (size == -1)
               size = l;
            else
               size += l;
         }
      }
      return size;
   }

   /**
    * Gives the content of the corresponding resource within the context of the given 
    * GridFS
    * @param gfs the GridFS in which we do the content retrieval
    * @param key the key of the resource for which we want the content
    * @param spoolConfig the configuration for spooling
    * @return the content of the corresponding resource
    * @throws IOException if an error occurs
    */
   public static InputStream getContent(GridFS gfs, String key, SpoolConfig spoolConfig) throws IOException
   {
      GridFSDBFile file = gfs.findOne(key);
      if (file == null)
         throw new FileNotFoundException("Could not find the key " + key);
      long fileSize = file.getLength();

      if (fileSize > spoolConfig.maxBufferSize)
      {
         SwapFile swapFile =
            SwapFile.get(spoolConfig.tempDirectory, key.replace('/', '_') + "." + System.currentTimeMillis() + "_"
               + SEQUENCE.incrementAndGet(), spoolConfig.fileCleaner);
         // spool GridFS Value content into swap file
         try
         {
            file.writeTo(swapFile);
         }
         finally
         {
            swapFile.spoolDone();
         }

         return new FileInputStream(swapFile);
      }
      else
      {
         InputStream is = file.getInputStream();
         try
         {
            byte[] data = new byte[(int)fileSize];
            byte[] buff =
               new byte[ValueFileIOHelper.IOBUFFER_SIZE > fileSize ? ValueFileIOHelper.IOBUFFER_SIZE : (int)fileSize];

            int rpos = 0;
            int read;

            while ((read = is.read(buff)) >= 0)
            {
               System.arraycopy(buff, 0, data, rpos, read);
               rpos += read;
            }

            return new ByteArrayInputStream(data);
         }
         finally
         {
            is.close();
         }
      }
   }
}
