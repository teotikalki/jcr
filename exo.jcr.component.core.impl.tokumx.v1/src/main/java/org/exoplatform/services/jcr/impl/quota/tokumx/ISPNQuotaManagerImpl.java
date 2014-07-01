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
package org.exoplatform.services.jcr.impl.quota.tokumx;

import org.exoplatform.container.configuration.ConfigurationManager;
import org.exoplatform.container.xml.InitParams;
import org.exoplatform.services.jcr.config.MappedParametrizedObjectEntry;
import org.exoplatform.services.jcr.config.RepositoryConfigurationException;
import org.exoplatform.services.jcr.impl.quota.QuotaManagerException;
import org.exoplatform.services.naming.InitialContextInitializer;
import org.exoplatform.services.rpc.RPCService;

import javax.jcr.RepositoryException;

/**
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class ISPNQuotaManagerImpl extends org.exoplatform.services.jcr.impl.quota.infinispan.ISPNQuotaManagerImpl
{

   /**
    *  The name to property cache configuration. 
    */
   public static final String INFINISPAN_MONGODB_CL_CONNECTION_URI = "infinispan-cl-cache.mongodb.connection.uri";

   public static final String INFINISPAN_MONGODB_CL_COLLECTION_NAME = "infinispan-cl-cache.mongodb.collection.name";

   public static final String INFINISPAN_MONGODB_CL_AUTO_COMMIT = "infinispan-cl-cache.mongodb.auto.commit";
   /**
    * ISPNQuotaManager constructor.
    */
   public ISPNQuotaManagerImpl(InitParams initParams, RPCService rpcService, ConfigurationManager cfm,
      InitialContextInitializer contextInitializer) throws RepositoryConfigurationException, QuotaManagerException
   {
      super(initParams, rpcService, cfm, contextInitializer);
   }

   /**
    * ISPNQuotaManager constructor.
    */
   public ISPNQuotaManagerImpl(InitParams initParams, ConfigurationManager cfm,
      InitialContextInitializer contextInitializer) throws RepositoryConfigurationException, QuotaManagerException
   {
      this(initParams, null, cfm, contextInitializer);
   }

   protected MappedParametrizedObjectEntry prepareISPNParameters(InitParams initParams) throws RepositoryException
   {
      MappedParametrizedObjectEntry qmEntry = new QuotaManagerEntry();

      qmEntry.putParameterValue(INFINISPAN_MONGODB_CL_COLLECTION_NAME, "quota");
      qmEntry.putBooleanParameter(INFINISPAN_MONGODB_CL_AUTO_COMMIT, Boolean.FALSE);

      qmEntry.putParameterValue(INFINISPAN_CLUSTER_NAME, DEFAULT_INFINISPANE_CLUSTER_NAME);
      qmEntry.putParameterValue(JGROUPS_CONFIGURATION, DEFAULT_JGROUPS_CONFIGURATION);

      putConfiguredValues(initParams, qmEntry);

      return qmEntry;
   }
}
