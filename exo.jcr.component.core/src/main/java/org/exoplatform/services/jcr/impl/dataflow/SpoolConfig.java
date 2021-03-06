/*
 * Copyright (C) 2009 eXo Platform SAS.
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
package org.exoplatform.services.jcr.impl.dataflow;

import org.exoplatform.commons.utils.PropertyManager;
import org.exoplatform.services.jcr.impl.util.io.FileCleaner;
import org.exoplatform.services.jcr.impl.util.io.FileCleanerHolder;
import org.exoplatform.services.jcr.storage.WorkspaceDataContainer;

import java.io.File;

/**
 * Simple class wrapper. Contains all needed variables for spooling input stream.
 * 
 * @author <a href="abazko@exoplatform.com">Anatoliy Bazko</a>
 * @version $Id: SpoolConfig.java 34360 2009-07-22 23:58:59Z tolusha $
 */
public class SpoolConfig
{
   public FileCleaner fileCleaner;

   public File tempDirectory = new File(PropertyManager.getProperty("java.io.tmpdir"));

   public int maxBufferSize = WorkspaceDataContainer.DEF_MAXBUFFERSIZE;

   /**
    * SpoolConfig constructor.
    */
   public SpoolConfig(FileCleaner fileCleaner)
   {
      this.fileCleaner = fileCleaner;
   }

   public static SpoolConfig getDefaultSpoolConfig()
   {
      return new SpoolConfig(FileCleanerHolder.getDefaultFileCleaner());
   }

}