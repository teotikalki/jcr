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
package org.exoplatform.services.jcr.ext.metadata;

import org.apache.commons.chain.Context;
import org.exoplatform.commons.utils.QName;
import org.exoplatform.container.ExoContainer;
import org.exoplatform.services.command.action.Action;
import org.exoplatform.services.document.DocumentReadException;
import org.exoplatform.services.document.DocumentReaderService;
import org.exoplatform.services.document.HandlerNotFoundException;
import org.exoplatform.services.ext.action.InvocationContext;
import org.exoplatform.services.jcr.core.nodetype.PropertyDefinitionData;
import org.exoplatform.services.jcr.datamodel.InternalQName;
import org.exoplatform.services.jcr.datamodel.NodeData;
import org.exoplatform.services.jcr.datamodel.PropertyData;
import org.exoplatform.services.jcr.impl.Constants;
import org.exoplatform.services.jcr.impl.core.JCRName;
import org.exoplatform.services.jcr.impl.core.LocationFactory;
import org.exoplatform.services.jcr.impl.core.NodeImpl;
import org.exoplatform.services.jcr.impl.core.PropertyImpl;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Properties;

import javax.jcr.PathNotFoundException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;

/**
 * Created by The eXo Platform SAS .
 * 
 * @author Gennady Azarenkov
 * @version $Id: AddMetadataAction.java 34445 2009-07-24 07:51:18Z dkatayev $
 */
public class AddMetadataAction implements Action
{
   /**
    * Logger.
    */
   private static Log LOG = ExoLogger.getLogger("exo.jcr.component.ext.AddMetadataAction");

   /**
    * {@inheritDoc}
    */
   public boolean execute(Context ctx) throws Exception
   {
      PropertyImpl property = (PropertyImpl)ctx.get(InvocationContext.CURRENT_ITEM);
      NodeImpl parent = getAndValidateParent(property);

      Content content = getContent(parent, property);
      try
      {
         if (!content.isEmpty())
         {
            Properties props = extractMetaInfoProperties(ctx, content);
            setJCRProperties(parent, props);
         }

         return false;
      }
      finally
      {
         content.destroy();
      }
   }

   /**
    * Extracts some metainfo properties from content using {@link DocumentReaderService}. 
    * 
    * @throws IllegalArgumentException if {@link DocumentReaderService} not configured
    */
   private Properties extractMetaInfoProperties(Context ctx, Content content) throws IllegalArgumentException
   {
      DocumentReaderService readerService =
         (DocumentReaderService)((ExoContainer)ctx.get(InvocationContext.EXO_CONTAINER))
            .getComponentInstanceOfType(DocumentReaderService.class);

      if (readerService == null)
      {
         throw new IllegalArgumentException("No DocumentReaderService configured for current container");
      }

      Properties props = new Properties();
      try
      {
         props = readerService.getDocumentReader(content.mimeType).getProperties(content.stream);
      }
      catch (HandlerNotFoundException e)
      {
         LOG.debug(e.getMessage());
      }
      catch (IOException e)
      {
         LOG.warn(e.getMessage());
      }
      catch (DocumentReadException e)
      {
         LOG.warn(e.getMessage());
      }

      return props;
   }

   /**
    * Sets metainfo properties as JCR properties to node.
    */
   private void setJCRProperties(NodeImpl parent, Properties props) throws Exception
   {
      if (!parent.isNodeType("dc:elementSet"))
      {
         parent.addMixin("dc:elementSet");
      }

      ValueFactory vFactory = parent.getSession().getValueFactory();
      LocationFactory lFactory = parent.getSession().getLocationFactory();

      for (Entry entry : props.entrySet())
      {
         QName qname = (QName)entry.getKey();
         JCRName jcrName = lFactory.createJCRName(new InternalQName(qname.getNamespace(), qname.getName()));

         PropertyDefinitionData definition =
            parent
               .getSession()
               .getWorkspace()
               .getNodeTypesHolder()
               .getPropertyDefinitions(jcrName.getInternalName(), ((NodeData)parent.getData()).getPrimaryTypeName(),
                  ((NodeData)parent.getData()).getMixinTypeNames()).getAnyDefinition();

         if (definition != null)
         {
            if (definition.isMultiple())
            {
               Value[] values = {createValue(entry.getValue(), vFactory)};
               parent.setProperty(jcrName.getAsString(), values);
            }
            else
            {
               Value value = createValue(entry.getValue(), vFactory);
               parent.setProperty(jcrName.getAsString(), value);
            }
         }
      }
   }

   /**
    * Validates if parent node type is {@link Constants#NT_RESOURCE}.
    */
   private NodeImpl getAndValidateParent(PropertyImpl property) throws Exception
   {
      NodeImpl parent = property.getParent();

      if (!parent.isNodeType("nt:resource"))
      {
         throw new Exception("Incoming node is not nt:resource type");
      }

      return parent;
   }

   private Content getContent(NodeImpl parent, PropertyImpl property) throws Exception
   {
      Content content = new Content();

      if (property.getInternalName().equals(Constants.JCR_DATA))
      {
         content.stream = new WrappedStream(((PropertyData)property.getData()).getValues().get(0).getAsStream());
         try
         {
            content.mimeType = parent.getProperty("jcr:mimeType").getString();
         }
         catch (PathNotFoundException e)
         {
            return content;
         }
      }
      else if (property.getInternalName().equals(Constants.JCR_MIMETYPE))
      {
         content.mimeType = property.getString();
         try
         {
            PropertyImpl propertyImpl = (PropertyImpl)parent.getProperty("jcr:data");
            content.stream = new WrappedStream(((PropertyData)propertyImpl.getData()).getValues().get(0).getAsStream());
         }
         catch (PathNotFoundException e)
         {
            return content;
         }
      }

      return content;
   }

   /**
    * Creates {@link Value} instance. Supported only 
    * {@link String}, {@link Calendar} and {@link Data} types.  
    */
   private Value createValue(Object obj, ValueFactory factory) throws ValueFormatException
   {
      if (obj instanceof String)
      {
         return factory.createValue((String)obj);
      }
      else if (obj instanceof Calendar)
      {
         return factory.createValue((Calendar)obj);
      }
      else if (obj instanceof Date)
      {
         Calendar cal = Calendar.getInstance();
         cal.setTime((Date)obj);
         return factory.createValue(cal);
      }
      else
      {
         throw new ValueFormatException("Unsupported value type " + obj.getClass());
      }
   }

   /**
    * Wraps mime-type and content represented by stream.
    */
   private class Content
   {
      String mimeType;

      WrappedStream stream;

      /**
       * Returns true if class contains all needed data.
       */
      boolean isEmpty() throws IOException
      {
         return mimeType == null || stream == null || stream.isEmpty();
      }

      /**
       * Frees all resources.
       */
      void destroy()
      {
         if (stream != null)
         {
            stream.close();
         }
      }
   }

   /** 
    * Simple implementation of {@link InputStream} with one 
    * additional method {@link #isEmpty()} to check if delegated
    * stream has bytes to read or doesn't. Might be useful don't pass 
    * empty streams to read data from. Famous usecase is Windows 
    * WebDAV client. First puts empty file and only then adds content.
    */
   private class WrappedStream extends InputStream
   {
      /**
       * Delegated stream.
       */
      private final InputStream delegated;

      /**
       * True means not empty buffer to read from.
       */
      private boolean consumed = true;

      /**
       * Buffer. Contains one read byte.
       */
      private int buffer;

      WrappedStream(InputStream delegated)
      {
         this.delegated = delegated;
      }

      /**
       * {@inheritDoc}
       */
      public int read() throws IOException
      {
         if (consumed)
         {
            return delegated.read();
         }
         else
         {
            consumed = true;
            return buffer;
         }
      }

      /**
       * Return true if stream is empty and false otherwise.
       */
      public boolean isEmpty() throws IOException
      {
         buffer = delegated.read();
         consumed = false;

         return buffer == -1;
      }

      /**
       * {@inheritDoc}
       */
      public void close()
      {
         try
         {
            delegated.close();
         }
         catch (IOException e)
         {
            LOG.error("Can't close input stream", e);
         }
      }
   }
}
