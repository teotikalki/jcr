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

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;

/**
 * A sub class of <code>com.mongodb.gridfs.GridFS</code> allowing to access to internal
 * variables.
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class GridFS extends com.mongodb.gridfs.GridFS
{

   /**
    * Creates a GridFS instance for the specified bucket
    * in the given database.  Set the preferred WriteConcern on the give DB with DB.setWriteConcern
    *
    * @see com.mongodb.WriteConcern
    * @param db database to work with
    * @param bucket bucket to use in the given database
    * @throws MongoException 
    */
   public GridFS(DB db, String bucket)
   {
      super(db, bucket);
   }

   /**
    * @return the files collection
    */
   DBCollection getFilesCollection()
   {
      return _filesCollection;
   }

   /**
    * @return the chunk collection
    */
   DBCollection getChunkCollection()
   {
      return _chunkCollection;
   }

   /**
    * @return the name of the field corresponding to the file name;
    */
   String getFileNameField()
   {
      return "filename";
   }

   /**
    * @return the name of the field corresponding to the id;
    */
   String getIdField()
   {
      return "_id";
   }
}
