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

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import org.exoplatform.commons.utils.PrivilegedFileHelper;
import org.exoplatform.commons.utils.PropertyManager;
import org.exoplatform.services.jcr.core.security.JCRRuntimePermissions;
import org.exoplatform.services.jcr.impl.backup.BackupException;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * An utility class for everything related to MongoDB/TokuMX
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class Utils
{

   private static final Log LOG = ExoLogger.getLogger("exo.jcr.component.core.impl.tokumx.v1.Utils");

   /**
    *  Name of fetch size property parameter in configuration.
    */
   private static final String FULL_BACKUP_JOB_FETCH_SIZE = "exo.jcr.component.ext.FullBackupJob.fetch-size";

   /**
    * The number of rows that should be fetched from the database
    */
   private static final int FETCH_SIZE;
   static
   {
      String size = PropertyManager.getProperty(FULL_BACKUP_JOB_FETCH_SIZE);
      int value = 1000;
      if (size != null)
      {
         try
         {
            value = Integer.valueOf(size);
         }
         catch (NumberFormatException e)
         {
            LOG.warn("The value of the property '" + FULL_BACKUP_JOB_FETCH_SIZE
               + "' must be an integer, the default value will be used.");
         }
      }
      FETCH_SIZE = value;
   }

   private Utils()
   {
   }

   /**
    * Provides the name of the collection using the provided prefix and suffix, that will be concatenated using 
    * underscore from which we will replace the invalid characters with underscores
    */
   public static String getCollectionName(String prefix, String suffix)
   {
      StringBuilder name = new StringBuilder();
      name.append(prefix);
      name.append('_');
      name.append(suffix.replace('$', '_').replace('\0', '_'));
      return name.toString();
   }

   /**
    * Backups the content of a collection and stores it into the provided content file
    * @param db the database from which we extract the content
    * @param collectionName the name of the collection to backup
    * @param contentFile the content file
    * @throws BackupException if any error occurs
    */
   public static void backup(DB db, String collectionName, File contentFile) throws BackupException
   {
      SecurityManager security = System.getSecurityManager();
      if (security != null)
      {
         security.checkPermission(JCRRuntimePermissions.MANAGE_REPOSITORY_PERMISSION);
      }
      db.requestStart();
      ObjectOutputStream oos = null;
      DBCursor cursor = null;
      try
      {
         db.requestEnsureConnection();
         DBCollection collection = db.getCollection(collectionName);
         cursor = collection.find().batchSize(FETCH_SIZE);
         oos = new ObjectOutputStream(PrivilegedFileHelper.fileOutputStream(contentFile));
         while (cursor.hasNext())
         {
            DBObject o = cursor.next();
            oos.writeBoolean(true);
            oos.writeObject(o);
         }
         oos.writeBoolean(false);
      }
      catch (Exception e)
      {
         throw new BackupException(e);
      }
      finally
      {
         try
         {
            if (cursor != null)
            {
               cursor.close();
            }
         }
         catch (Exception e)
         {
            LOG.warn("Could not close the cursor: " + e.getMessage());
         }
         try
         {
            if (oos != null)
            {
               oos.close();
            }
         }
         catch (IOException e)
         {
            LOG.warn("Could not close the object output stream: " + e.getMessage());
         }
         db.requestDone();
      }
   }

   /**
    * Drops a given collection of a given Database
    * @param db the database that owns the collection
    * @param collectionName the collection to drop
    * @throws BackupException if any error occurs
    */
   public static void clean(DB db, String collectionName) throws BackupException
   {
      SecurityManager security = System.getSecurityManager();
      if (security != null)
      {
         security.checkPermission(JCRRuntimePermissions.MANAGE_REPOSITORY_PERMISSION);
      }
      db.requestStart();
      try
      {
         db.requestEnsureConnection();
         if (db.collectionExists(collectionName))
         {
            LOG.info("Drop the collection '" + collectionName + "'");
            db.getCollection(collectionName).drop();
         }
      }
      catch (Exception e)
      {
         throw new BackupException(e);
      }
      finally
      {
         db.requestDone();
      }
   }
}
