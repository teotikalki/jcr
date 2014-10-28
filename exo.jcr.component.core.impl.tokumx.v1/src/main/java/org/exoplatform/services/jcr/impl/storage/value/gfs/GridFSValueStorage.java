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
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import org.exoplatform.services.jcr.config.RepositoryConfigurationException;
import org.exoplatform.services.jcr.impl.storage.value.ValueDataResourceHolder;
import org.exoplatform.services.jcr.impl.util.io.FileCleaner;
import org.exoplatform.services.jcr.storage.value.ValueIOChannel;
import org.exoplatform.services.jcr.storage.value.ValueStoragePlugin;
import org.exoplatform.services.jcr.storage.value.ValueStorageURLStreamHandler;
import org.exoplatform.services.jcr.util.tokumx.Utils;

import java.io.IOException;
import java.util.Properties;

/**
 * This class is the implementation of a value storage based on the Grid FS available
 * in MongoDB
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class GridFSValueStorage extends ValueStoragePlugin
{

   /**
    * Suffix used in collection names
    */
   public final static String COLLECTION_NAME_SUFFIX = "collection-name-suffix";

   /**
    * The Mongo connection URI
    */
   public final static String CONNECTION_URI = "connection-uri";

   /**
    * The client to use to access to the MongoDB server
    */
   private MongoClient mongo;

   /**
    * The URI used to access to Mongo DB
    */
   private MongoClientURI uri;

   /**
    * The corresponding GridFS
    */
   private GridFS gridFS;

   private GridFSURLStreamHandler handler;

   private final FileCleaner cleaner;

   public GridFSValueStorage(FileCleaner cleaner)
   {
      this.cleaner = cleaner;
   }

   /**
    * Constructor used for testing purpose
    */
   GridFSValueStorage(GridFS gridFS, FileCleaner cleaner)
   {
      this.gridFS = gridFS;
      this.cleaner = cleaner;
      this.handler = new GridFSURLStreamHandler(gridFS);
   }

   /**
    * {@inheritDoc}
    */
   public void init(Properties props, ValueDataResourceHolder resources) throws RepositoryConfigurationException,
      IOException
   {
      String collectionNameSuffix = props.getProperty(COLLECTION_NAME_SUFFIX, getRepository() + "_" + getWorkspace());
      String collectionName = Utils.getCollectionName("jcr_gfs", collectionNameSuffix);
      String connectionURI = props.getProperty(CONNECTION_URI);
      if (connectionURI == null || connectionURI.isEmpty())
         throw new RepositoryConfigurationException("The connection URI has not been set");
      this.uri = new MongoClientURI(connectionURI);
      String databaseName = uri.getDatabase() == null || uri.getDatabase().isEmpty() ? "jcr" : uri.getDatabase();
      this.mongo = new MongoClient(uri);
      DB mongoDb = mongo.getDB(databaseName);
      this.gridFS = new GridFS(mongoDb, collectionName);
      gridFS.getFilesCollection().ensureIndex(new BasicDBObject(GridFSValueUtil.GROUP_KEY, 1));
      this.handler = new GridFSURLStreamHandler(gridFS);
   }

   /**
    * Resets the GridFS
    */
   void resetGridFS()
   {
      if (gridFS == null)
         return;
      this.gridFS = new GridFS(gridFS.getDB(), gridFS.getBucketName());
      gridFS.getFilesCollection().ensureIndex(new BasicDBObject(GridFSValueUtil.GROUP_KEY, 1));
      this.handler = new GridFSURLStreamHandler(gridFS);
   }

   /**
    * Gives a new instance of the related DB to ensure that we don't have the
    * GridFS context
    */
   DB getNewDBInstance()
   {
      try
      {
         MongoClient mongo = new MongoClient(uri);
         return mongo.getDB(gridFS.getDB().getName());
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   /**
    * {@inheritDoc}
    */
   public ValueIOChannel openIOChannel() throws IOException
   {
      return new GridFSValueIOChannel(this);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   protected ValueStorageURLStreamHandler getURLStreamHandler()
   {
      return handler;
   }

   GridFS getGridFS()
   {
      return gridFS;
   }

   FileCleaner getCleaner()
   {
      return cleaner;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   protected void finalize() throws Throwable
   {
      try
      {
         // Ensure that we release all the resources before garbage collecting
         // this instance
         if (mongo != null)
            mongo.close();
      }
      finally
      {
         super.finalize();
      }
   }
}
