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
package org.exoplatform.services.jcr.tokumx;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;

import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This is a transactional equivalent of <code>MongoDBCacheStore</code>
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
@CacheLoaderMetadata(configurationClass = TokuMXCacheStoreConfig.class)
public class TokuMXCacheStore extends AbstractCacheStore
{

   private static final Log LOG = ExoLogger.getLogger("exo.jcr.component.core.impl.tokumx.v1.TokuMXCacheStore");

   private static final boolean trace = LOG.isTraceEnabled();

   private TokuMXCacheStoreConfig cfg;

   private MongoClient mongo;

   private DBCollection collection;

   private DB mongoDb;

   private static final String ID_FIELD = "_id";

   private static final String TIMESTAMP_FIELD = "timestamp";

   private static final String VALUE_FIELD = "value";

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException
   {
      super.init(config, cache, m);
      this.cfg = (TokuMXCacheStoreConfig)config;
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException
   {
      mongoDb.requestStart();
      try
      {
         mongoDb.requestEnsureConnection();
         BasicDBObject searchObject = new BasicDBObject();
         searchObject.put(TIMESTAMP_FIELD, new BasicDBObject("$gt", 0).append("$lt", System.currentTimeMillis()));
         this.collection.remove(searchObject);
      }
      finally
      {
         mongoDb.requestDone();
      }
   }

   public void store(InternalCacheEntry entry) throws CacheLoaderException
   {
      if (trace)
      {
         LOG.trace("Adding entry: {}", entry);
      }
      byte[] id = objectToByteBuffer(entry.getKey());
      mongoDb.requestStart();
      try
      {
         mongoDb.requestEnsureConnection();
         if (this.findById(id) == null)
         {
            BasicDBObject entryObject = this.createDBObject(id);
            entryObject.put(VALUE_FIELD, objectToByteBuffer(entry));
            entryObject.put(TIMESTAMP_FIELD, entry.getExpiryTime());
            this.collection.update(entryObject, entryObject, true, false);
         }
         else
         {
            BasicDBObject updater = new BasicDBObject(VALUE_FIELD, objectToByteBuffer(entry));
            updater.append(TIMESTAMP_FIELD, entry.getExpiryTime());
            this.collection.update(this.createDBObject(id), updater);
         }
      }
      finally
      {
         mongoDb.requestDone();
      }
   }

   private BasicDBObject createDBObject(Object key)
   {
      BasicDBObject dbObject = new BasicDBObject();
      dbObject.put(ID_FIELD, key);
      return dbObject;
   }

   public void fromStream(ObjectInput inputStream) throws CacheLoaderException
   {
      Object objectFromStream;
      try
      {
         objectFromStream = getMarshaller().objectFromObjectStream(inputStream);
      }
      catch (InterruptedException e)
      {
         Thread.currentThread().interrupt();
         throw new CacheLoaderException("Unable to unmarshall the input stream", e);
      }
      catch (Exception e)
      {
         throw new CacheLoaderException("Unable to unmarshall the input stream", e);
      }
      if (objectFromStream instanceof InternalCacheEntry)
      {
         InternalCacheEntry ice = (InternalCacheEntry)objectFromStream;
         this.store(ice);
      }
      else if (objectFromStream instanceof Set)
      {
         @SuppressWarnings("unchecked")
         Set<InternalCacheEntry> internalCacheEntries = (Set<InternalCacheEntry>)objectFromStream;
         for (InternalCacheEntry ice : internalCacheEntries)
         {
            this.store(ice);
         }
      }
   }

   public void toStream(ObjectOutput outputStream) throws CacheLoaderException
   {
      try
      {
         Set<InternalCacheEntry> internalCacheEntries = this.loadAll();
         if (internalCacheEntries.size() == 1)
         {
            getMarshaller().objectToObjectStream(internalCacheEntries.iterator().next(), outputStream);
         }
         else if (internalCacheEntries.size() > 1)
         {
            getMarshaller().objectToObjectStream(internalCacheEntries, outputStream);
         }
      }
      catch (Exception e)
      {
         throw new CacheLoaderException("Unable to unmarshall the output stream", e);
      }
   }

   public void clear()
   {
      mongoDb.requestStart();
      try
      {
         mongoDb.requestEnsureConnection();
         this.collection.drop();
      }
      finally
      {
         mongoDb.requestDone();
      }
   }

   public void drop()
   {
      this.mongoDb.dropDatabase();
   }

   private DBObject findById(byte[] id) throws CacheLoaderException
   {
      try
      {
         return this.collection.findOne(new BasicDBObject(ID_FIELD, id));
      }
      catch (MongoException e)
      {
         throw new CacheLoaderException("Unable to load [" + id.toString() + "]", e);
      }
   }

   public boolean remove(Object key) throws CacheLoaderException
   {
      byte[] id = objectToByteBuffer(key);
      mongoDb.requestStart();
      try
      {
         mongoDb.requestEnsureConnection();
         if (this.findById(id) == null)
         {
            return false;
         }
         else
         {
            this.collection.remove(this.createDBObject(id));
            return true;
         }
      }
      finally
      {
         mongoDb.requestDone();
      }
   }

   private Object unmarshall(DBObject dbObject, String field) throws CacheLoaderException
   {
      try
      {
         return getMarshaller().objectFromByteBuffer((byte[])dbObject.get(field));
      }
      catch (IOException e)
      {
         throw new CacheLoaderException("Unable to unmarshall [" + dbObject + "]", e);
      }
      catch (ClassNotFoundException e)
      {
         throw new CacheLoaderException("Unable to unmarshall [" + dbObject + "]", e);
      }
   }

   private InternalCacheEntry createInternalCacheEntry(DBObject dbObject) throws CacheLoaderException
   {
      return (InternalCacheEntry)unmarshall(dbObject, VALUE_FIELD);
   }

   private byte[] objectToByteBuffer(Object key) throws CacheLoaderException
   {
      try
      {
         return getMarshaller().objectToByteBuffer(key);
      }
      catch (IOException e)
      {
         throw new CacheLoaderException("Unable to marshall [" + key + "]", e);
      }
      catch (InterruptedException e)
      {
         throw new CacheLoaderException("Unable to marshall [" + key + "]", e);
      }
   }

   public InternalCacheEntry load(Object key) throws CacheLoaderException
   {
      byte[] id = objectToByteBuffer(key);
      mongoDb.requestStart();
      try
      {
         mongoDb.requestEnsureConnection();
         BasicDBObject dbObject = this.createDBObject(id);
         DBObject[] orArray = new BasicDBObject[2];
         orArray[0] = new BasicDBObject(TIMESTAMP_FIELD, new BasicDBObject("$gte", System.currentTimeMillis()));
         orArray[1] = new BasicDBObject(TIMESTAMP_FIELD, -1);
         dbObject.append("$or", orArray);
         DBObject rawResult = this.collection.findOne(dbObject);
         if (rawResult != null)
         {
            return this.createInternalCacheEntry(rawResult);
         }
         else
         {
            return null;
         }
      }
      finally
      {
         mongoDb.requestDone();
      }
   }

   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException
   {
      mongoDb.requestStart();
      try
      {
         mongoDb.requestEnsureConnection();
         DBCursor dbObjects = this.collection.find();
         Set<InternalCacheEntry> entries = new HashSet<InternalCacheEntry>(dbObjects.count());
         while (dbObjects.hasNext())
         {
            DBObject next = dbObjects.next();
            InternalCacheEntry ice = this.createInternalCacheEntry(next);
            entries.add(ice);
         }
         return entries;
      }
      finally
      {
         mongoDb.requestDone();
      }
   }

   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException
   {
      mongoDb.requestStart();
      try
      {
         mongoDb.requestEnsureConnection();
         Set<InternalCacheEntry> values = new HashSet<InternalCacheEntry>();
         DBCursor dbObjects = this.collection.find().limit(numEntries);
         for (DBObject dbObject : dbObjects)
         {
            InternalCacheEntry ice = this.createInternalCacheEntry(dbObject);
            values.add(ice);
         }
         return values;
      }
      finally
      {
         mongoDb.requestDone();
      }
   }

   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException
   {
      mongoDb.requestStart();
      try
      {
         mongoDb.requestEnsureConnection();
         BasicDBObject excludeObject = new BasicDBObject();
         Set<Object> values = new HashSet<Object>();
         DBCursor keyObjects = null;
         BasicDBObject idRestrictionObject = new BasicDBObject(ID_FIELD, 1);
         if (keysToExclude != null)
         {
            byte[][] exclusionArray = new byte[keysToExclude.size()][];
            Iterator<Object> iterator = keysToExclude.iterator();
            int i = 0;

            while (iterator.hasNext())
            {
               Object next = iterator.next();
               exclusionArray[i++] = objectToByteBuffer(next);
            }
            excludeObject.put(ID_FIELD, new BasicDBObject("$nin", exclusionArray));
            keyObjects = this.collection.find(excludeObject);
         }
         else
         {
            keyObjects = this.collection.find(new BasicDBObject(), idRestrictionObject);
         }
         for (DBObject rawObject : keyObjects)
         {
            values.add(this.unmarshall(rawObject, ID_FIELD));
         }
         return values;
      }
      finally
      {
         mongoDb.requestDone();
      }
   }

   @Override
   public void start() throws CacheLoaderException
   {
      super.start();
      String connectionURI = cfg.getConnectionURI();
      if (connectionURI == null || connectionURI.isEmpty())
         throw new CacheLoaderException("The connection URI has not been set");
      LOG.trace("Connection URI has been set to '{}'", connectionURI);
      String collectionName = cfg.getCollectionName();
      if (collectionName == null || collectionName.isEmpty())
         throw new CacheLoaderException("The name of the collection has not been set");
      LOG.trace("Collection name has been set to '{}'", collectionName);
      LOG.trace("Auto commit has been set to '{}'", cfg.isAutoCommit());
      String databaseName;
      try
      {
         MongoClientURI uri = new MongoClientURI(connectionURI);
         this.mongo = new MongoClient(uri);
         databaseName = uri.getDatabase() == null || uri.getDatabase().isEmpty() ? "jcr" : uri.getDatabase();
      }
      catch (Exception e)
      {
         throw new CacheLoaderException("Unable to find or initialize a connection to the MongoDB server", e);
      }
      mongoDb = extractDatabase(databaseName);
      this.collection = mongoDb.getCollection(collectionName);

   }

   @Override
   public void stop() throws CacheLoaderException
   {
      super.stop();
      LOG.debug("Closing connection to MongoDB sever instance");
      this.mongo.close();
   }

   private DB extractDatabase(String databaseName) throws CacheLoaderException
   {
      try
      {
         LOG.debug("Connecting to Mongo database named {}", databaseName);
         return this.mongo.getDB(databaseName);
      }
      catch (MongoException e)
      {
         throw new CacheLoaderException("Unable to connect to the database " + databaseName, e);
      }
   }

   /**
    * {@inheritDoc}
    */
   public Class<? extends CacheLoaderConfig> getConfigurationClass()
   {
      return TokuMXCacheStoreConfig.class;
   }

   /**
    * @see org.infinispan.loaders.AbstractCacheStore#prepare(java.util.List, org.infinispan.transaction.xa.GlobalTransaction, boolean)
    */
   @Override
   public void prepare(List<? extends Modification> mods, GlobalTransaction tx, boolean isOnePhase)
      throws CacheLoaderException
   {
      mongoDb.requestStart();
      mongoDb.requestEnsureConnection();
      if (!cfg.isAutoCommit())
      {
         // Starts the transaction
         BasicDBObject command = new BasicDBObject("beginTransaction", Boolean.TRUE).append("isolation", "mvcc");
         mongoDb.command(command);
      }
      // Put modifications
      super.prepare(mods, tx, true);
      // commit if it's one phase only
      if (isOnePhase)
         commit(tx);
   }

   /**
    * @see org.infinispan.loaders.AbstractCacheStore#rollback(org.infinispan.transaction.xa.GlobalTransaction)
    */
   @Override
   public void rollback(GlobalTransaction tx)
   {
      try
      {
         super.rollback(tx);
         if (!cfg.isAutoCommit())
            mongoDb.command("rollbackTransaction");
      }
      finally
      {
         mongoDb.requestDone();
      }
   }

   /**
    * @see org.infinispan.loaders.AbstractCacheStore#commit(org.infinispan.transaction.xa.GlobalTransaction)
    */
   @Override
   public void commit(GlobalTransaction tx) throws CacheLoaderException
   {
      try
      {
         super.commit(tx);
         if (!cfg.isAutoCommit())
            mongoDb.command("commitTransaction");
      }
      finally
      {
         mongoDb.requestDone();
      }
   }

}
