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
package org.exoplatform.services.jcr.impl.storage.tokumx.indexing;

import org.exoplatform.commons.utils.PropertyManager;
import org.exoplatform.services.jcr.datamodel.NodeDataIndexing;
import org.exoplatform.services.jcr.impl.core.query.NodeDataIndexingIterator;
import org.exoplatform.services.jcr.impl.storage.statistics.StatisticsWorkspaceStorageConnection;
import org.exoplatform.services.jcr.impl.storage.tokumx.MXWorkspaceDataContainer;
import org.exoplatform.services.jcr.impl.storage.tokumx.MXWorkspaceStorageConnection;
import org.exoplatform.services.jcr.storage.WorkspaceStorageConnection;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.RepositoryException;

/**
 * Iterator for fetching NodeData from database with all properties and its values.
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class MXNodeDataIndexingIterator implements NodeDataIndexingIterator
{

   /**
    * The workspace data container. Allows to open workspace storage connection. 
    */
   private final MXWorkspaceDataContainer dataContainer;

   /**
    * The amount of the rows which could be retrieved from database for once.
    */
   private final int pageSize;

   /**
    * The current offset in database.
    */
   private final AtomicInteger offset = new AtomicInteger(0);

   /**
    * Indicates if not all records have been read from database.
    */
   private final AtomicBoolean hasNext = new AtomicBoolean(true);

   /**
    * The last node Id retrieved from the DB
    */
   private final AtomicReference<String> lastNodeId = new AtomicReference<String>();

   /**
    * The current page index
    */
   private final AtomicInteger page = new AtomicInteger();

   /**
    * Logger.
    */
   protected static final Log LOG = ExoLogger
      .getLogger("exo.jcr.component.core.impl.tokumx.v1.MXNodeDataIndexingIterator");

   /**
    * Constructor MXNodeDataIndexingIterator.
    * 
    */
   public MXNodeDataIndexingIterator(MXWorkspaceDataContainer dataContainer, int pageSize) throws RepositoryException
   {
      this.dataContainer = dataContainer;
      this.pageSize = pageSize;
   }

   /**
    * {@inheritDoc}
    */
   public List<NodeDataIndexing> next() throws RepositoryException
   {
      if (!hasNext())
      {
         // avoid unnecessary request to database
         return new ArrayList<NodeDataIndexing>();
      }

      WorkspaceStorageConnection connection = dataContainer.openConnection(true);
      MXWorkspaceStorageConnection conn;
      if (connection instanceof StatisticsWorkspaceStorageConnection)
      {
         conn =
            (MXWorkspaceStorageConnection)((StatisticsWorkspaceStorageConnection)connection)
               .getNestedWorkspaceStorageConnection();
      }
      else
      {
         conn = (MXWorkspaceStorageConnection)connection;
      }
      try
      {
         int currentOffset;
         String currentLastNodeId;
         int currentPage;
         synchronized (this)
         {
            currentOffset = offset.getAndAdd(pageSize);
            currentLastNodeId = lastNodeId.get();
            currentPage = page.incrementAndGet();
         }
         if (!hasNext())
         {
            // avoid unnecessary request to database
            return new ArrayList<NodeDataIndexing>();
         }
         long time = 0;
         if (PropertyManager.isDevelopping())
         {
            time = System.currentTimeMillis();
         }
         List<NodeDataIndexing> result = conn.getNodesAndProperties(currentLastNodeId, currentOffset, pageSize);
         if (PropertyManager.isDevelopping())
         {
            LOG.info("Page = " + currentPage + " Offset = " + currentOffset + " LastNodeId = '" + currentLastNodeId
               + "', query time = " + (System.currentTimeMillis() - time) + " ms, from '"
               + (result.isEmpty() ? "unknown" : result.get(0).getIdentifier()) + "' to '"
               + (result.isEmpty() ? "unknown" : result.get(result.size() - 1).getIdentifier()) + "'");
         }
         hasNext.compareAndSet(true, result.size() == pageSize);
         if (hasNext())
         {
            synchronized (this)
            {
               lastNodeId.set(result.get(result.size() - 1).getIdentifier());
               offset.set((page.get() - currentPage) * pageSize);
            }
         }

         return result;
      }
      finally
      {
         conn.close();
      }
   }

   /**
    * {@inheritDoc}
    */
   public boolean hasNext()
   {
      return hasNext.get();
   }

}
