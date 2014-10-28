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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import org.exoplatform.commons.utils.PrivilegedFileHelper;
import org.exoplatform.services.jcr.core.security.JCRRuntimePermissions;
import org.exoplatform.services.jcr.impl.backup.BackupException;
import org.exoplatform.services.jcr.impl.backup.DataRestore;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipInputStream;

/**
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class TokuMXDataRestore implements DataRestore
{

   private static final Log LOG = ExoLogger.getLogger("exo.jcr.component.core.impl.tokumx.v1.TokuMXDataRestore");

   /**
    * The maximum possible batch size.
    */
   private final int MAXIMUM_BATCH_SIZE = 1000;

   /**
    * The target database
    */
   protected final DB database;

   /**
    * The name of the collection
    */
   private final String collectionName;

   /**
    * The name of the zip file in which the content is stored
    */
   private final String contentZipFile;

   /**
    * The name of the content file
    */
   private final String contentFile;

   private final File storageDir;

   public TokuMXDataRestore(DB database, String collectionName, String contentZipFile, String contentFile, File storageDir)
   {
      this.storageDir = storageDir;
      this.database = database;
      this.collectionName = collectionName;
      this.contentZipFile = contentZipFile;
      this.contentFile = contentFile;
      database.requestStart();
      database.requestEnsureConnection();
   }

   /**
    * {@inheritDoc}
    */
   public void clean() throws BackupException
   {
      if (database.collectionExists(collectionName))
      {
         DBCollection collection = database.getCollection(collectionName);
         LOG.info("Drop the collection '" + collectionName + "'");
         collection.drop();
      }
      createCollection(database);
   }

   /**
    * Creates the collection
    */
   protected DBCollection createCollection(DB db)
   {
      LOG.debug("The collection '{}' doesn't exist so it will be created", collectionName);
      DBCollection collection = db.createCollection(collectionName, new BasicDBObject());
      return collection;
   }

   /**
    * {@inheritDoc}
    */
   public void restore() throws BackupException
   {
      SecurityManager security = System.getSecurityManager();
      if (security != null)
      {
         security.checkPermission(JCRRuntimePermissions.MANAGE_REPOSITORY_PERMISSION);
      }
      ObjectInputStream ois = null;
      try
      {
         DBCollection collection = database.getCollection(collectionName);

         ZipInputStream zis = PrivilegedFileHelper.zipInputStream(new File(storageDir, contentZipFile));
         while (!zis.getNextEntry().getName().endsWith(contentFile));
         ois = new ObjectInputStream(zis);
         List<DBObject> objects = new ArrayList<DBObject>();
         while (ois.readBoolean())
         {
            DBObject o = (DBObject)ois.readObject();
            objects.add(o);
            if (objects.size() >= MAXIMUM_BATCH_SIZE)
            {
               collection.insert(objects);
               objects.clear();
            }
         }
         if (!objects.isEmpty())
            collection.insert(objects);
         postRestore(collection);
      }
      catch (Exception e)
      {
         throw new BackupException(e);
      }
      finally
      {
         if (ois != null)
         {
            try
            {
               ois.close();
            }
            catch (IOException e)
            {
               LOG.warn("Could not close the object input stream: " + e.getMessage());
            }
         }
      }
   }

   /**
    * Executes an action after the restore
    */
   protected void postRestore(DBCollection collection)
   {
      
   }

   /**
    * {@inheritDoc}
    */
   public void commit() throws BackupException
   {
      // We do nothing as we want to use the auto commit mode
   }

   /**
    * {@inheritDoc}
    */
   public void rollback() throws BackupException
   {
      // We do nothing as we want to use the auto commit mode
   }

   /**
    * {@inheritDoc}
    */
   public void close() throws BackupException
   {
      database.requestDone();
   }

}
