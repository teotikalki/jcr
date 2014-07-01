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

import org.infinispan.loaders.LockSupportCacheStoreConfig;

/**
 * The configuration class for {@link TokuMXCacheStore}
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class TokuMXCacheStoreConfig extends LockSupportCacheStoreConfig
{

   /**
    * The serial version UID
    */
   private static final long serialVersionUID = -4793981078426271139L;

   /**
    * The connection URI
    */
   private String connectionURI;

   /**
    * The name of the collection
    */
   private String collectionName;

   /**
    * Indicates whether the auto commit mode is expected or not
    */
   private boolean autoCommit;
 
   public TokuMXCacheStoreConfig()
   {
      super.setCacheLoaderClassName(TokuMXCacheStore.class.getName());
   }

   /**
    * Gives the connection URI
    */
   public String getConnectionURI()
   {
      return connectionURI;
   }

   /**
    * Sets the connection URI
    */
   public void setConnectionURI(String connectionURI)
   {
      this.connectionURI = connectionURI;
   }

   /**
    * @return the collectionName
    */
   public String getCollectionName()
   {
      return collectionName;
   }

   /**
    * @param collectionName the collectionName to set
    */
   public void setCollectionName(String collectionName)
   {
      this.collectionName = collectionName;
   }

   /**
    * @return the autoCommit
    */
   public boolean isAutoCommit()
   {
      return autoCommit;
   }

   /**
    * @param autoCommit the autoCommit to set
    */
   public void setAutoCommit(boolean autoCommit)
   {
      this.autoCommit = autoCommit;
   }
}
