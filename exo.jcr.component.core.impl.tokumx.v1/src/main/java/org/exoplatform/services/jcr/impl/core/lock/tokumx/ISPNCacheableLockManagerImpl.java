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
package org.exoplatform.services.jcr.impl.core.lock.tokumx;

import org.exoplatform.container.configuration.ConfigurationManager;
import org.exoplatform.services.jcr.config.RepositoryConfigurationException;
import org.exoplatform.services.jcr.config.WorkspaceEntry;
import org.exoplatform.services.jcr.impl.core.lock.LockRemoverHolder;
import org.exoplatform.services.jcr.impl.core.lock.LockTableHandler;
import org.exoplatform.services.jcr.impl.dataflow.persistent.WorkspacePersistentDataManager;
import org.exoplatform.services.jcr.infinispan.ISPNCacheFactory;
import org.exoplatform.services.jcr.tokumx.Utils;
import org.exoplatform.services.naming.InitialContextInitializer;
import org.exoplatform.services.transaction.TransactionService;
import org.infinispan.loaders.CacheLoaderManager;

import java.io.Serializable;

import javax.jcr.RepositoryException;
import javax.transaction.TransactionManager;

/**
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class ISPNCacheableLockManagerImpl extends
   org.exoplatform.services.jcr.impl.core.lock.infinispan.ISPNCacheableLockManagerImpl
{

   /**
    *  The name to property cache configuration. 
    */
   public static final String INFINISPAN_MONGODB_CL_CONNECTION_URI = "infinispan-cl-cache.mongodb.connection.uri";

   public static final String INFINISPAN_MONGODB_CL_COLLECTION_NAME = "infinispan-cl-cache.mongodb.collection.name";

   public static final String INFINISPAN_MONGODB_CL_AUTO_COMMIT = "infinispan-cl-cache.mongodb.auto.commit";

   public ISPNCacheableLockManagerImpl(WorkspacePersistentDataManager dataManager, WorkspaceEntry config,
      InitialContextInitializer context, TransactionService transactionService, ConfigurationManager cfm,
      LockRemoverHolder lockRemoverHolder) throws RepositoryConfigurationException, RepositoryException
   {
      this(dataManager, config, context, transactionService.getTransactionManager(), cfm, lockRemoverHolder);
   }

   public ISPNCacheableLockManagerImpl(WorkspacePersistentDataManager dataManager, WorkspaceEntry config,
      InitialContextInitializer context, ConfigurationManager cfm, LockRemoverHolder lockRemoverHolder)
      throws RepositoryConfigurationException, RepositoryException
   {
      this(dataManager, config, context, (TransactionManager)null, cfm, lockRemoverHolder);
   }

   public ISPNCacheableLockManagerImpl(WorkspacePersistentDataManager dataManager, WorkspaceEntry config,
      InitialContextInitializer context, TransactionManager transactionManager, ConfigurationManager cfm,
      LockRemoverHolder lockRemoverHolder) throws RepositoryConfigurationException, RepositoryException
   {
      super(dataManager, config, context, transactionManager, cfm, lockRemoverHolder);
   }

   /**
    * Initializes the lock manager
    */
   protected void init(WorkspaceEntry config, TransactionManager transactionManager, ConfigurationManager cfm)
      throws RepositoryConfigurationException, RepositoryException
   {
      // create cache using custom factory
      ISPNCacheFactory<Serializable, Object> factory =
         new ISPNCacheFactory<Serializable, Object>(cfm, transactionManager);

      String collectionNameSuffix =
         config.getLockManager().getParameterValue(INFINISPAN_MONGODB_CL_COLLECTION_NAME, config.getUniqueName());
      String collectionName = Utils.getCollectionName("lm", collectionNameSuffix);
      LOG.debug("The collection name has been set to {}.", collectionName);
      config.getLockManager().putParameterValue(INFINISPAN_MONGODB_CL_COLLECTION_NAME, collectionName);
      cache =
         factory.createCache("L" + config.getUniqueName().replace("_", ""), config.getLockManager()).getAdvancedCache();
   }

   /**
    * {@inheritDoc}
    */
   public LockTableHandler getLockTableHandler()
   {
      CacheLoaderManager cacheLoaderManager =
         cache.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      return new ISPNLockTableHandler(cacheLoaderManager.getCacheStore());
   }
}
