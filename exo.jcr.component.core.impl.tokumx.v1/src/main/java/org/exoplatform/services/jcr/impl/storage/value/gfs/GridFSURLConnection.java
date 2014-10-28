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

import org.exoplatform.commons.utils.SecurityHelper;
import org.exoplatform.services.jcr.impl.dataflow.SpoolConfig;
import org.exoplatform.services.jcr.storage.value.ValueStorageURLConnection;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

/**
 * This implementation of an {@link ValueStorageURLConnection} allows to get a content from
 * the GridFS
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
class GridFSURLConnection extends ValueStorageURLConnection
{
   /**
    * This value is used to know whether a value has been defined or node
    */
   private static final int UNDEFINED = -16;

   /**
    * The client that we use to access to the GridFS
    */
   private final GridFS gridFs;

   /**
    * The content length of the resource
    */
   private int contentLength = UNDEFINED;

   /**
    * @param url
    */
   GridFSURLConnection(GridFS gridFs, URL url)
   {
      this(gridFs, null, url);
   }

   /**
    * @param url
    */
   GridFSURLConnection(GridFS gridFs, String idResource, URL url)
   {
      super(url);
      this.gridFs = gridFs;
      this.idResource = idResource;
   }

   /**
    * @see java.net.URLConnection#connect()
    */
   public void connect() throws IOException
   {
   }

   /**
    * @see java.net.URLConnection#getContentLength()
    */
   @Override
   public int getContentLength()
   {
      if (contentLength == UNDEFINED)
      {
         contentLength = SecurityHelper.doPrivilegedAction(new PrivilegedAction<Integer>()
         {
            public Integer run()
            {
               return (int)GridFSValueUtil.getContentLength(gridFs, idResource);
            }
         });
         if (contentLength == UNDEFINED)
            contentLength = -1;
      }

      return contentLength;
   }

   /**
    * @see java.net.URLConnection#getInputStream()
    */
   @Override
   public InputStream getInputStream() throws IOException
   {
      return new GridFSURLConnectionInputStream();
   }

   /**
    * Class allowing to extract the content from the GridFS
    */
   private class GridFSURLConnectionInputStream extends InputStream
   {
      private long start;

      private InputStream delegate;

      private int diff;

      @Override
      public int read() throws IOException
      {
         return SecurityHelper.doPrivilegedIOExceptionAction(new PrivilegedExceptionAction<Integer>()
         {

            public Integer run() throws Exception
            {
               int result = getDelegate().read();
               if (result != -1)
               {
                  diff--;
               }
               return result;
            }
         });
      }

      /**
       * @see java.io.InputStream#available()
       */
      @Override
      public int available() throws IOException
      {
         int available = getContentLength();
         available += diff;
         return available <= 0 ? 0 : available;
      }

      /**
       * @see java.io.InputStream#read(byte[], int, int)
       */
      @Override
      public int read(final byte[] b, final int off, final int len) throws IOException
      {
         return SecurityHelper.doPrivilegedIOExceptionAction(new PrivilegedExceptionAction<Integer>()
         {

            public Integer run() throws Exception
            {
               InputStream delegate = getDelegate();
               int result = delegate.read(b, off, len);
               if (result != -1)
               {
                  diff -= result;
                  while (result < len)
                  {
                     int delta = delegate.read(b, off + result, len - result);
                     if (delta == -1)
                     {
                        break;
                     }
                     diff -= delta;
                     result += delta;
                  }
               }
               return result;
            }
         });

      }

      /**
       * @see java.io.InputStream#skip(long)
       */
      @Override
      public long skip(long n) throws IOException
      {
         if (n <= 0)
            return 0;
         diff -= n;
         return start = n;
      }

      /**
       * @see java.io.InputStream#close()
       */
      @Override
      public void close() throws IOException
      {
         if (delegate != null)
         {
            delegate.close();
         }
      }

      /**
       * Gives the delegate InputStream, if it has not been set yet, it will first initialize it
       */
      private InputStream getDelegate() throws IOException
      {
         if (delegate == null)
         {
            delegate = GridFSValueUtil.getContent(gridFs, idResource, SpoolConfig.getDefaultSpoolConfig());
            if (start > 0)
            {
               delegate.skip(start);
            }
         }
         return delegate;
      }
   }
}
