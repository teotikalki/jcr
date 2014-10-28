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

import java.io.File;

/**
 * An abstract representation of file on GridFS.
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class GridFSFile extends File
{
   /**
    * The serial version UID
    */
   private static final long serialVersionUID = 7086363673617059636L;

   /**
    * The client that we use to access to MongoDB
    */
   private final GridFS gfs;

   /**
    * The key corresponding to the resource
    */
   private final String key;

   public GridFSFile(String key, GridFS gfs)
   {
      super((String)null, key);
      this.gfs = gfs;
      this.key = key;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isDirectory()
   {
      return false;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isFile()
   {
      return true;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean delete()
   {
      return GridFSValueUtil.delete(gfs, key);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean exists()
   {
      try
      {
         return GridFSValueUtil.exists(gfs, key);
      }
      catch (Exception e)
      {
         // If we cannot know if it exists or not, we will assume that it doesn't exist
         return false;
      }
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((key == null) ? 0 : key.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (getClass() != obj.getClass())
         return false;
      GridFSFile other = (GridFSFile)obj;
      if (key == null)
      {
         if (other.key != null)
            return false;
      }
      else if (!key.equals(other.key))
         return false;
      return true;
   }
}
