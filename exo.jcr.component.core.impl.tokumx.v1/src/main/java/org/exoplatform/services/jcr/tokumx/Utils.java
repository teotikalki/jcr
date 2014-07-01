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

/**
 * An utility class for everything related to MongoDB/TokuMX
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class Utils
{

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
}
