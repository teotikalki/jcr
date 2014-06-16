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
package org.exoplatform.services.jcr.impl.storage.tokumx;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoException.DuplicateKey;
import com.mongodb.WriteResult;

import org.exoplatform.services.jcr.access.AccessControlEntry;
import org.exoplatform.services.jcr.access.AccessControlList;
import org.exoplatform.services.jcr.core.ExtendedPropertyType;
import org.exoplatform.services.jcr.dataflow.ItemState;
import org.exoplatform.services.jcr.dataflow.persistent.PersistedNodeData;
import org.exoplatform.services.jcr.dataflow.persistent.PersistedPropertyData;
import org.exoplatform.services.jcr.datamodel.Identifier;
import org.exoplatform.services.jcr.datamodel.IllegalACLException;
import org.exoplatform.services.jcr.datamodel.IllegalNameException;
import org.exoplatform.services.jcr.datamodel.IllegalPathException;
import org.exoplatform.services.jcr.datamodel.InternalQName;
import org.exoplatform.services.jcr.datamodel.ItemData;
import org.exoplatform.services.jcr.datamodel.ItemType;
import org.exoplatform.services.jcr.datamodel.NodeData;
import org.exoplatform.services.jcr.datamodel.PropertyData;
import org.exoplatform.services.jcr.datamodel.QPath;
import org.exoplatform.services.jcr.datamodel.QPathEntry;
import org.exoplatform.services.jcr.datamodel.ValueData;
import org.exoplatform.services.jcr.impl.Constants;
import org.exoplatform.services.jcr.impl.core.itemfilters.QPathEntryFilter;
import org.exoplatform.services.jcr.impl.dataflow.NamePersistedValueData;
import org.exoplatform.services.jcr.impl.dataflow.PathPersistedValueData;
import org.exoplatform.services.jcr.impl.dataflow.PermissionPersistedValueData;
import org.exoplatform.services.jcr.impl.dataflow.ReferencePersistedValueData;
import org.exoplatform.services.jcr.impl.dataflow.SpoolConfig;
import org.exoplatform.services.jcr.impl.dataflow.ValueDataUtil;
import org.exoplatform.services.jcr.impl.dataflow.ValueDataUtil.ValueDataWrapper;
import org.exoplatform.services.jcr.impl.dataflow.persistent.ACLHolder;
import org.exoplatform.services.jcr.impl.dataflow.persistent.BooleanPersistedValueData;
import org.exoplatform.services.jcr.impl.dataflow.persistent.ByteArrayPersistedValueData;
import org.exoplatform.services.jcr.impl.dataflow.persistent.CalendarPersistedValueData;
import org.exoplatform.services.jcr.impl.dataflow.persistent.ChangedSizeHandler;
import org.exoplatform.services.jcr.impl.dataflow.persistent.DoublePersistedValueData;
import org.exoplatform.services.jcr.impl.dataflow.persistent.LongPersistedValueData;
import org.exoplatform.services.jcr.impl.dataflow.persistent.PersistedValueData;
import org.exoplatform.services.jcr.impl.dataflow.persistent.SimplePersistedSize;
import org.exoplatform.services.jcr.impl.dataflow.persistent.StreamPersistedValueData;
import org.exoplatform.services.jcr.impl.dataflow.persistent.StringPersistedValueData;
import org.exoplatform.services.jcr.impl.storage.JCRInvalidItemStateException;
import org.exoplatform.services.jcr.impl.storage.jdbc.PrimaryTypeNotFoundException;
import org.exoplatform.services.jcr.impl.storage.value.ValueStorageNotFoundException;
import org.exoplatform.services.jcr.impl.storage.value.fs.operations.ValueFileIOHelper;
import org.exoplatform.services.jcr.impl.util.JCRDateFormat;
import org.exoplatform.services.jcr.impl.util.io.SwapFile;
import org.exoplatform.services.jcr.storage.WorkspaceStorageConnection;
import org.exoplatform.services.jcr.storage.value.ValueIOChannel;
import org.exoplatform.services.jcr.storage.value.ValueStoragePluginProvider;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import javax.jcr.InvalidItemStateException;
import javax.jcr.ItemExistsException;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFormatException;

/**
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class MXWorkspaceStorageConnection implements WorkspaceStorageConnection
{
   private static final Log LOG = ExoLogger.getLogger("exo.jcr.component.core.MXWorkspaceStorageConnection");

   static final String ID = "Id";

   static final String PARENT_ID = "Pid";

   static final String NAME = "Name";

   static final String VERSION = "Vsn";

   static final String INDEX = "Idx";

   private static final String SIZE = "Size";

   private static final String PROPERTY_TYPE_BINARY = "Bin";

   private static final String PROPERTY_TYPE_BOOLEAN = "Bol";

   private static final String PROPERTY_TYPE_DATE = "Date";

   private static final String PROPERTY_TYPE_DOUBLE = "Dbl";

   private static final String PROPERTY_TYPE_LONG = "Lng";

   private static final String PROPERTY_TYPE_STRING = "Str";

   private static final String PROPERTY_TYPE_NAME = "Na";

   private static final String PROPERTY_TYPE_PATH = "Pa";

   private static final String PROPERTY_TYPE_PERMISSION = "Per";

   static final String PROPERTY_TYPE_REFERENCE = "Ref";

   private static final String VALUE_STORAGE = "Vsto";

   static final String ORDER_NUMBER = "Onum";

   static final String IS_NODE = "Ino";

   private static final String TYPE = "Type";

   private static final String IS_MULTI_VALUED = "Imv";

   static final String VALUES = "Vals";

   private static final String JCR_PRIMARYTYPE = "PType";

   private static final String JCR_MIXINTYPES = "MTypes";

   private static final String EXO_OWNER = "Owner";

   private static final String EXO_PERMISSIONS = "Perms";

   private static final Map<String, String> MAIN_PROPERTIES;

   private static final Map<Integer, String> PROPERTY_TYPES;
   static
   {
      Map<String, String> mainProperties = new HashMap<String, String>(5, 1f);
      mainProperties.put(Constants.JCR_PRIMARYTYPE.getAsString(), JCR_PRIMARYTYPE);
      mainProperties.put(Constants.JCR_MIXINTYPES.getAsString(), JCR_MIXINTYPES);
      mainProperties.put(Constants.EXO_OWNER.getAsString(), EXO_OWNER);
      mainProperties.put(Constants.EXO_PERMISSIONS.getAsString(), EXO_PERMISSIONS);
      MAIN_PROPERTIES = Collections.unmodifiableMap(mainProperties);

      Map<Integer, String> propertyTypes = new HashMap<Integer, String>(12, 1f);
      propertyTypes.put(PropertyType.BINARY, PROPERTY_TYPE_BINARY);
      propertyTypes.put(PropertyType.UNDEFINED, PROPERTY_TYPE_BINARY);
      propertyTypes.put(PropertyType.BOOLEAN, PROPERTY_TYPE_BOOLEAN);
      propertyTypes.put(PropertyType.DATE, PROPERTY_TYPE_DATE);
      propertyTypes.put(PropertyType.DOUBLE, PROPERTY_TYPE_DOUBLE);
      propertyTypes.put(PropertyType.LONG, PROPERTY_TYPE_LONG);
      propertyTypes.put(PropertyType.NAME, PROPERTY_TYPE_NAME);
      propertyTypes.put(PropertyType.PATH, PROPERTY_TYPE_PATH);
      propertyTypes.put(ExtendedPropertyType.PERMISSION, PROPERTY_TYPE_PERMISSION);
      propertyTypes.put(PropertyType.STRING, PROPERTY_TYPE_STRING);
      propertyTypes.put(PropertyType.REFERENCE, PROPERTY_TYPE_REFERENCE);
      PROPERTY_TYPES = Collections.unmodifiableMap(propertyTypes);
   }

   private static final String MAP = "function () { var size = 0; for (var i = 0; i < this." + VALUES
      + ".length; i++) { size += this." + VALUES + "[i].Size; } emit(\"Size\", size);}";

   private static final String REDUCE =
      "function (key, values) { var result = 0; for (var idx = 0; idx < values.length; idx++) { result += values[idx] } return result; }";

   private static final WriteValueHelper WRITE_VALUE_HELPER = new WriteValueHelper();

   /**
    * Helper.
    */
   private static class WriteValueHelper extends ValueFileIOHelper
   {
      /**
       * {@inheritDoc}
       */
      @Override
      public long writeStreamedValue(File file, ValueData value) throws IOException
      {
         return super.writeStreamedValue(file, value);
      }
   }

   /**
    * The target collection
    */
   private final DB db;

   /**
    * The target collection
    */
   private final DBCollection collection;

   private final List<ValueIOChannel> valueChanges;

   private List<Callable<Void>> updateSizeTasks;

   /**
    * Value storage provider
    */
   private final ValueStoragePluginProvider valueStorageProvider;

   private final boolean readOnly;

   private boolean closed;

   private boolean autoCommit;

   /**
    * Spool config
    */
   private final SpoolConfig spoolConfig;

   MXWorkspaceStorageConnection(DB db, String collectionName, boolean readOnly,
      ValueStoragePluginProvider valueStorageProvider, SpoolConfig spoolConfig)
   {
      this.db = db;
      this.collection = db.getCollection(collectionName);
      this.readOnly = readOnly;
      db.requestEnsureConnection();
      if (readOnly)
      {
         db.setReadOnly(readOnly);
      }
      else
      {
         autoCommit = false;
         beginTransaction();
      }
      this.valueStorageProvider = valueStorageProvider;
      this.spoolConfig = spoolConfig;
      this.valueChanges = new ArrayList<ValueIOChannel>();
   }

   private void beginTransaction()
   {
      if (!autoCommit)
      {
         // Starts the transaction
         BasicDBObject command = new BasicDBObject("beginTransaction", Boolean.TRUE).append("isolation", "mvcc");
         db.command(command);
      }
   }

   /**
    * {@inheritDoc}
    */
   public boolean hasItemData(NodeData parentData, QPathEntry name, ItemType itemType) throws RepositoryException,
      IllegalStateException
   {
      checkIfOpened();
      BasicDBObject query =
         new BasicDBObject(PARENT_ID, parentData == null ? Constants.ROOT_PARENT_UUID : parentData.getIdentifier())
            .append(NAME, name.getAsString()).append(INDEX, name.getIndex());
      BasicDBObject sort = null;
      switch (itemType)
      {
         case NODE : {
            query.append(IS_NODE, Boolean.TRUE);
            break;
         }
         case PROPERTY : {
            query.append(IS_NODE, Boolean.FALSE);
            break;
         }
         default : {
            sort = new BasicDBObject(IS_NODE, 1);
         }
      }
      return collection.findOne(query, new BasicDBObject(ID, Boolean.TRUE), sort) != null;
   }

   /**
    * {@inheritDoc}
    */
   public ItemData getItemData(NodeData parentData, QPathEntry name, ItemType itemType) throws RepositoryException,
      IllegalStateException
   {
      checkIfOpened();
      BasicDBObject query =
         new BasicDBObject(PARENT_ID, parentData == null ? Constants.ROOT_PARENT_UUID : parentData.getIdentifier())
            .append(NAME, name.getAsString()).append(INDEX, name.getIndex());
      BasicDBObject sort = null;
      switch (itemType)
      {
         case NODE : {
            query.append(IS_NODE, Boolean.TRUE);
            break;
         }
         case PROPERTY : {
            query.append(IS_NODE, Boolean.FALSE);
            break;
         }
         default : {
            sort = new BasicDBObject(IS_NODE, 1);
         }
      }
      DBObject item = collection.findOne(query, null, sort);
      if (item != null)
      {
         try
         {
            boolean isNode = (Boolean)item.get(IS_NODE);
            return itemData(parentData.getQPath(), item, isNode, parentData.getACL());
         }
         catch (IOException e)
         {
            throw new RepositoryException(e);
         }
      }
      return null;
   }

   /**
    * Build ItemData.
    * 
    * @param parentPath
    *          - parent path
    * @param item
    *          database - DBObject with Item record(s)
    * @param isNode
    *          - Item type (Node or Property)
    * @param parentACL
    *          - parent ACL
    * @return ItemData instance
    * @throws RepositoryException
    *           Repository error
    * @throws IOException
    *           I/O error
    */
   private ItemData itemData(QPath parentPath, DBObject item, boolean isNode, AccessControlList parentACL)
      throws RepositoryException, IOException
   {
      String cid = (String)item.get(ID);
      String cname = (String)item.get(NAME);
      int cversion = (Integer)item.get(VERSION);

      String cpid = (String)item.get(PARENT_ID);

      try
      {
         if (isNode)
         {
            int cindex = (Integer)item.get(INDEX);
            int cnordernumb = (Integer)item.get(ORDER_NUMBER);
            return loadNodeRecord(item, parentPath, cname, cid, cpid, cindex, cversion, cnordernumb, parentACL);
         }

         int cptype = (Integer)item.get(TYPE);
         boolean cpmultivalued = (Boolean)item.get(IS_MULTI_VALUED);
         return loadPropertyRecord(item, parentPath, cname, cid, cpid, cversion, cptype, cpmultivalued);
      }
      catch (InvalidItemStateException e)
      {
         throw new InvalidItemStateException("FATAL: Can't build item path for name " + cname + " id: " + cid + ". "
            + e);
      }
   }

   /**
    * Load NodeData record.
    * 
    * @param node
    *          the DBObject that contains all the data of the node
    * @param parentPath
    *          parent path
    * @param cname
    *          Node name
    * @param cid
    *          Node id
    * @param cpid
    *          Node parent id
    * @param cindex
    *          Node index
    * @param cversion
    *          Node persistent version
    * @param cnordernumb
    *          Node order number
    * @param parentACL
    *          Node parent ACL
    * @return PersistedNodeData
    * @throws RepositoryException
    *           Repository error
    */
   protected PersistedNodeData loadNodeRecord(DBObject node, QPath parentPath, String cname, String cid, String cpid,
      int cindex, int cversion, int cnordernumb, AccessControlList parentACL) throws RepositoryException
   {
      try
      {
         InternalQName qname = InternalQName.parse(cname);

         QPath qpath;
         String parentCid;
         if (parentPath != null)
         {
            // get by parent and name
            qpath = QPath.makeChildPath(parentPath, qname, cindex, cid);
            parentCid = cpid;
         }
         else
         {
            // get by id
            if (cpid.equals(Constants.ROOT_PARENT_UUID))
            {
               // root node
               qpath = Constants.ROOT_PATH;
               parentCid = null;
            }
            else
            {
               qpath = QPath.makeChildPath(traverseQPath(cpid), qname, cindex, cid);
               parentCid = cpid;
            }
         }

         @SuppressWarnings("unchecked")
         List<String> values = (List<String>)node.get(JCR_PRIMARYTYPE);
         try
         {

            if (values == null || values.isEmpty())
            {
               throw new PrimaryTypeNotFoundException("FATAL ERROR primary type record not found. Node "
                  + qpath.getAsString() + ", id " + cid, null);
            }

            String data = values.get(0);
            InternalQName ptName = InternalQName.parse(data);

            // MIXIN
            MixinInfo mixins = readMixins(node, cid);

            // ACL
            AccessControlList acl; // NO DEFAULT values!

            if (mixins.hasOwneable())
            {
               // has own owner
               if (mixins.hasPrivilegeable())
               {
                  // and permissions
                  acl = new AccessControlList(readACLOwner(node, cid), readACLPermisions(node, cid));
               }
               else if (parentACL != null)
               {
                  // use permissions from existed parent
                  acl =
                     new AccessControlList(readACLOwner(node, cid), parentACL.hasPermissions()
                        ? parentACL.getPermissionEntries() : null);
               }
               else
               {
                  // have to search nearest ancestor permissions in ACL manager
                  // acl = new AccessControlList(readACLOwner(node, cid), traverseACLPermissions(cpid));
                  acl = new AccessControlList(readACLOwner(node, cid), null);
               }
            }
            else if (mixins.hasPrivilegeable())
            {
               // has own permissions
               if (mixins.hasOwneable())
               {
                  // and owner
                  acl = new AccessControlList(readACLOwner(node, cid), readACLPermisions(node, cid));
               }
               else if (parentACL != null)
               {
                  // use owner from existed parent
                  acl = new AccessControlList(parentACL.getOwner(), readACLPermisions(node, cid));
               }
               else
               {
                  // have to search nearest ancestor owner in ACL manager
                  // acl = new AccessControlList(traverseACLOwner(cpid), readACLPermisions(node, cid));
                  acl = new AccessControlList(null, readACLPermisions(node, cid));
               }
            }
            else
            {
               if (parentACL != null)
               {
                  // construct ACL from existed parent ACL
                  acl =
                     new AccessControlList(parentACL.getOwner(), parentACL.hasPermissions()
                        ? parentACL.getPermissionEntries() : null);
               }
               else
               {
                  // have to search nearest ancestor owner and permissions in ACL manager
                  // acl = traverseACL(cpid);
                  acl = null;
               }
            }

            return new PersistedNodeData(cid, qpath, parentCid, cversion, cnordernumb, ptName, mixins.mixinNames(), acl);
         }
         catch (IllegalACLException e)
         {
            throw new RepositoryException("FATAL ERROR Node " + cid + " " + qpath.getAsString()
               + " has wrong formed ACL. ", e);
         }
      }
      catch (IllegalNameException e)
      {
         throw new RepositoryException(e);
      }
   }

   /**
    * Read mixins from database.
    * 
    * @param cid
    *          - Item id (internal)
    * @return MixinInfo
    * @throws IllegalNameException
    *           if nodetype name in mixin record is wrong
    */
   protected MixinInfo readMixins(DBObject node, String cid) throws IllegalNameException
   {
      @SuppressWarnings("unchecked")
      List<String> values = (List<String>)node.get(JCR_MIXINTYPES);
      List<InternalQName> mts = null;
      boolean owneable = false;
      boolean privilegeable = false;
      if (values != null)
      {
         mts = new ArrayList<InternalQName>();
         for (String mxnb : values)
         {
            InternalQName mxn = InternalQName.parse(mxnb);
            mts.add(mxn);

            if (!privilegeable && Constants.EXO_PRIVILEGEABLE.equals(mxn))
            {
               privilegeable = true;
            }
            else if (!owneable && Constants.EXO_OWNEABLE.equals(mxn))
            {
               owneable = true;
            }
         }
      }
      return new MixinInfo(mts, owneable, privilegeable);
   }

   /**
    * Mixin types description (internal use).
    * 
    */
   public class MixinInfo
   {

      /**
       * OWNEABLE constant.
       */
      static final int OWNEABLE = 0x0001; // bits 0001

      /**
       * PRIVILEGEABLE constant.
       */
      static final int PRIVILEGEABLE = 0x0002; // bits 0010

      /**
       * OWNEABLE_PRIVILEGEABLE constant.
       */
      static final int OWNEABLE_PRIVILEGEABLE = OWNEABLE | PRIVILEGEABLE; // bits 0011

      /**
       * Mixin types.
       */
      final List<InternalQName> mixinTypes;

      /**
       * oexo:owneable flag.
       */
      final boolean owneable;

      /**
       * exo:privilegeable flag.
       */
      final boolean privilegeable;

      /**
       * Parent Id.
       */
      final String parentId = null;

      /**
       * MixinInfo constructor.
       * 
       * @param mixinTypes
       *          mixin types
       * @param owneable
       *          exo:owneable flag
       * @param privilegeable
       *          exo:privilegeable flag
       */
      public MixinInfo(List<InternalQName> mixinTypes, boolean owneable, boolean privilegeable)
      {
         this.mixinTypes = mixinTypes;
         this.owneable = owneable;
         this.privilegeable = privilegeable;
      }

      /**
       * Return Mixin names array.
       * 
       * @return InternalQName[] Mixin names array
       */
      public InternalQName[] mixinNames()
      {
         if (mixinTypes != null)
         {
            InternalQName[] mns = new InternalQName[mixinTypes.size()];
            mixinTypes.toArray(mns);
            return mns;
         }
         else
         {
            return new InternalQName[0];
         }
      }

      /**
       * Tell is exo:privilegeable.
       * 
       * @return boolean
       */
      public boolean hasPrivilegeable()
      {
         return privilegeable;
      }

      /**
       * Tell is exo:owneable.
       * 
       * @return boolean
       */
      public boolean hasOwneable()
      {
         return owneable;
      }

      public String getParentId()
      {
         return parentId;
      }
   }

   /**
    * Return permission values or throw an exception. We assume the node is mix:privilegeable.
    * 
    * @param cid
    *          Node id
    * @return list of ACL entries
    * @throws IllegalACLException
    *           if property exo:permissions is not found for node
    */
   protected List<AccessControlEntry> readACLPermisions(DBObject node, String cid) throws IllegalACLException
   {
      List<AccessControlEntry> naPermissions = new ArrayList<AccessControlEntry>();
      @SuppressWarnings("unchecked")
      List<String> values = (List<String>)node.get(EXO_PERMISSIONS);
      if (values != null)
      {
         for (String value : values)
         {
            StringTokenizer parser = new StringTokenizer(value, AccessControlEntry.DELIMITER);
            naPermissions.add(new AccessControlEntry(parser.nextToken(), parser.nextToken()));
         }

         return naPermissions;
      }
      else
      {
         throw new IllegalACLException("Property exo:permissions is not found for node with id: " + cid);
      }
   }

   /**
    * Return owner value or throw an exception. We assume the node is mix:owneable.
    * 
    * @param cid
    *          Node id
    * @return ACL owner
    * @throws IllegalACLException
    *           Property exo:owner is not found for node
    */
   protected String readACLOwner(DBObject node, String cid) throws IllegalACLException
   {
      @SuppressWarnings("unchecked")
      List<String> values = (List<String>)node.get(EXO_OWNER);
      if (values != null && !values.isEmpty())
      {
         return values.get(0);
      }
      else
      {
         throw new IllegalACLException("Property exo:owner is not found for node with id: " + cid);
      }
   }

   /**
    * Build Item path by id.
    * 
    * @param cpid
    *          - Item id (container id)
    * @return Item QPath
    * @throws InvalidItemStateException
    *           - if parent not found
    * @throws IllegalNameException
    *           - if name on the path is wrong
    */
   protected QPath traverseQPath(String cpid) throws InvalidItemStateException, IllegalNameException
   {
      // get item by Identifier usecase 
      List<QPathEntry> qrpath = new ArrayList<QPathEntry>(); // reverted path
      String caid = cpid; // container ancestor id
      do
      {
         BasicDBObject query = new BasicDBObject(ID, caid);
         DBObject parent =
            collection.findOne(query,
               new BasicDBObject(PARENT_ID, Boolean.TRUE).append(NAME, Boolean.TRUE).append(INDEX, Boolean.TRUE));
         if (parent == null)
         {
            throw new InvalidItemStateException("Parent not found, uuid: " + caid);
         }

         QPathEntry qpe =
            new QPathEntry(InternalQName.parse((String)parent.get(NAME)), (Integer)parent.get(INDEX), caid);
         qrpath.add(qpe);
         String pId = (String)parent.get(PARENT_ID);

         if (caid.equals(pId))
         {
            throw new InvalidItemStateException("An item with id='" + caid + "' is its own parent");
         }
         caid = pId;
      }
      while (!caid.equals(Constants.ROOT_PARENT_UUID));

      QPathEntry[] qentries = new QPathEntry[qrpath.size()];
      int qi = 0;
      for (int i = qrpath.size() - 1; i >= 0; i--)
      {
         qentries[qi++] = qrpath.get(i);
      }
      return new QPath(qentries);
   }

   /**
    * Load PropertyData record.
    * 
    * @param property
    *          the DBObject that contains all the data of the property
    * @param parentPath
    *          parent path
    * @param cname
    *          Property name
    * @param cid
    *          Property id
    * @param cpid
    *          Property parent id
    * @param cversion
    *          Property persistent version
    * @param cptype
    *          Property type
    * @param cpmultivalued
    *          Property multivalued status
    * @return PersistedPropertyData
    * @throws RepositoryException
    *           Repository error
    * @throws IOException
    *           I/O error
    */
   protected PersistedPropertyData loadPropertyRecord(DBObject property, QPath parentPath, String cname, String cid,
      String cpid, int cversion, int cptype, boolean cpmultivalued) throws RepositoryException, IOException
   {

      try
      {
         QPath qpath =
            QPath.makeChildPath(parentPath == null ? traverseQPath(cpid) : parentPath, InternalQName.parse(cname));

         String identifier = cid;

         List<ValueDataWrapper> data = readValues(property, cid, cptype, identifier, cversion);

         long size = 0;
         List<ValueData> values = new ArrayList<ValueData>();
         for (ValueDataWrapper vdDataWrapper : data)
         {
            values.add(vdDataWrapper.value);
            size += vdDataWrapper.size;
         }

         PersistedPropertyData pdata =
            new PersistedPropertyData(identifier, qpath, cpid, cversion, cptype, cpmultivalued, values,
               new SimplePersistedSize(size));

         return pdata;
      }
      catch (IllegalNameException e)
      {
         throw new RepositoryException(e);
      }
   }

   /**
    * Read Property Values.
    * 
    * @param property
    *          the DBObject that contains all the data of the property
    * @param identifier
    *          property identifier
    * @param cid
    *          Property id
    * @param pdata
    *          PropertyData
    * @return list of ValueData
    * @throws IOException
    *           i/O error

    * @throws ValueStorageNotFoundException
    *           if no such storage found with Value storageId
    */
   private List<ValueDataWrapper> readValues(DBObject property, String cid, int cptype, String identifier, int cversion)
      throws IOException, ValueStorageNotFoundException
   {
      List<ValueDataWrapper> data = new ArrayList<ValueDataWrapper>();

      @SuppressWarnings("unchecked")
      List<DBObject> valueRecords = (List<DBObject>)property.get(VALUES);
      if (valueRecords != null)
      {
         for (DBObject valueRecord : valueRecords)
         {
            int orderNum = (Integer)valueRecord.get(INDEX);
            String storageId = (String)valueRecord.get(VALUE_STORAGE);

            ValueDataWrapper vdWrapper;
            if (storageId == null)
            {
               vdWrapper = new ValueDataWrapper();
               vdWrapper.size = (Long)valueRecord.get(SIZE);
               vdWrapper.value = createValueData(cptype, orderNum, valueRecord);
            }
            else
            {
               vdWrapper = readValueData(identifier, orderNum, cptype, storageId);
            }

            data.add(vdWrapper);
         }
      }
      return data;
   }

   /**
    * Creates value data depending on its type. It avoids storing unnecessary bytes in memory 
    * every time.
    * 
    * @param type
    *          property data type, can be either {@link PropertyType} or {@link ExtendedPropertyType}
    * @param orderNumber
    *          value data order number
    * @param valueRecord
    *          value record from which the data needs to be extracted according to the data type
    */
   private static PersistedValueData createValueData(int type, int orderNumber, DBObject valueRecord)
      throws IOException
   {
      Object data = null;
      switch (type)
      {
         case PropertyType.BINARY :
         case PropertyType.UNDEFINED :
            data = valueRecord.get(PROPERTY_TYPE_BINARY);
            return new ByteArrayPersistedValueData(orderNumber, (byte[])data);

         case PropertyType.BOOLEAN :
            data = valueRecord.get(PROPERTY_TYPE_BOOLEAN);
            return new BooleanPersistedValueData(orderNumber, (Boolean)data);

         case PropertyType.DATE :
            try
            {
               data = valueRecord.get(PROPERTY_TYPE_DATE);
               return new CalendarPersistedValueData(orderNumber, JCRDateFormat.parse((String)data));
            }
            catch (ValueFormatException e)
            {
               throw new IOException("Can't create Calendar value with " + data, e);
            }

         case PropertyType.DOUBLE :
            data = valueRecord.get(PROPERTY_TYPE_DOUBLE);
            return new DoublePersistedValueData(orderNumber, (Double)data);

         case PropertyType.LONG :
            data = valueRecord.get(PROPERTY_TYPE_LONG);
            return new LongPersistedValueData(orderNumber, (Long)data);

         case PropertyType.NAME :
            try
            {
               data = valueRecord.get(PROPERTY_TYPE_NAME);
               return new NamePersistedValueData(orderNumber, InternalQName.parse((String)data));
            }
            catch (IllegalNameException e)
            {
               throw new IOException(e.getMessage(), e);
            }

         case PropertyType.PATH :
            try
            {
               data = valueRecord.get(PROPERTY_TYPE_PATH);
               return new PathPersistedValueData(orderNumber, QPath.parse((String)data));
            }
            catch (IllegalPathException e)
            {
               throw new IOException(e.getMessage(), e);
            }

         case PropertyType.REFERENCE :
            data = valueRecord.get(PROPERTY_TYPE_REFERENCE);
            return new ReferencePersistedValueData(orderNumber, new Identifier((String)data));

         case PropertyType.STRING :
            data = valueRecord.get(PROPERTY_TYPE_STRING);
            return new StringPersistedValueData(orderNumber, (String)data);

         case ExtendedPropertyType.PERMISSION :
            data = valueRecord.get(PROPERTY_TYPE_PERMISSION);
            return new PermissionPersistedValueData(orderNumber, AccessControlEntry.parse((String)data));

         default :
            throw new IllegalStateException("Unknown property type " + type);
      }
   }

   /**
    * Read ValueData from External Storage.
    * 
    * @param pdata
    *          PropertyData
    * @param orderNumber
    *          Value order number
    * @param type
    *          property type         
    * @param storageId
    *          external Value storage id
    * @return ValueData
    * @throws IOException
    *           I/O error
    * @throws ValueStorageNotFoundException
    *           if no such storage found with Value storageId
    */
   protected ValueDataWrapper readValueData(String identifier, int orderNumber, int type, String storageId)
      throws IOException, ValueStorageNotFoundException
   {
      ValueIOChannel channel = this.valueStorageProvider.getChannel(storageId);
      try
      {
         return channel.read(identifier, orderNumber, type, spoolConfig);
      }
      finally
      {
         channel.close();
      }
   }

   /**
    * {@inheritDoc}
    */
   public ItemData getItemData(String identifier) throws RepositoryException, IllegalStateException
   {
      checkIfOpened();
      BasicDBObject query = new BasicDBObject(ID, identifier);

      DBObject item = collection.findOne(query);
      if (item != null)
      {
         try
         {
            boolean isNode = (Boolean)item.get(IS_NODE);
            return itemData(null, item, isNode, null);
         }
         catch (IOException e)
         {
            throw new RepositoryException(e);
         }
      }
      return null;
   }

   /**
    * {@inheritDoc}
    */
   public List<NodeData> getChildNodesData(NodeData parent) throws RepositoryException, IllegalStateException
   {
      checkIfOpened();
      BasicDBObject query = new BasicDBObject(PARENT_ID, parent.getIdentifier()).append(IS_NODE, Boolean.TRUE);
      DBCursor cursor = collection.find(query).sort(new BasicDBObject(ORDER_NUMBER, 1));
      List<NodeData> result = null;
      try
      {
         result = new ArrayList<NodeData>();
         while (cursor.hasNext())
         {
            result.add((NodeData)itemData(parent.getQPath(), cursor.next(), true, parent.getACL()));
         }
      }
      catch (IOException e)
      {
         throw new RepositoryException(e);
      }
      finally
      {
         cursor.close();
      }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   public List<NodeData> getChildNodesData(NodeData parent, List<QPathEntryFilter> pattern) throws RepositoryException,
      IllegalStateException
   {
      checkIfOpened();
      if (pattern.isEmpty())
      {
         return Collections.emptyList();
      }
      BasicDBObject query = new BasicDBObject(PARENT_ID, parent.getIdentifier()).append(IS_NODE, Boolean.TRUE);
      List<BasicDBObject> orClause = new ArrayList<BasicDBObject>();
      for (QPathEntryFilter filter : pattern)
      {
         BasicDBObject clause = new BasicDBObject();
         QPathEntry entry = filter.getQPathEntry();
         String name = entry.getAsString();
         if (name.contains("*"))
         {
            Pattern pat = Pattern.compile(escapeSpecialChars(name));
            clause.append(NAME, pat);
         }
         else
         {
            clause.append(NAME, name);
         }
         if (entry.getIndex() != -1)
         {
            clause.append(INDEX, entry.getIndex());
         }
         orClause.add(clause);
      }
      query.append("$or", orClause);
      DBCursor cursor = collection.find(query);
      List<NodeData> result = null;
      try
      {
         result = new ArrayList<NodeData>();
         while (cursor.hasNext())
         {
            result.add((NodeData)itemData(parent.getQPath(), cursor.next(), true, parent.getACL()));
         }
      }
      catch (IOException e)
      {
         throw new RepositoryException(e);
      }
      finally
      {
         cursor.close();
      }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   public int getChildNodesCount(NodeData parent) throws RepositoryException
   {
      checkIfOpened();
      return (int)collection.count(new BasicDBObject(PARENT_ID, parent.getIdentifier()).append(IS_NODE, Boolean.TRUE));
   }

   /**
    * {@inheritDoc}
    */
   public int getLastOrderNumber(NodeData parent) throws RepositoryException
   {
      checkIfOpened();
      BasicDBObject query = new BasicDBObject(PARENT_ID, parent.getIdentifier()).append(IS_NODE, Boolean.TRUE);
      DBCursor cursor =
         collection.find(query, new BasicDBObject(ORDER_NUMBER, Boolean.TRUE))
            .sort(new BasicDBObject(ORDER_NUMBER, -1)).limit(1);
      try
      {
         if (cursor.hasNext())
         {
            return (Integer)cursor.next().get(ORDER_NUMBER);
         }
         return -1;
      }
      finally
      {
         cursor.close();
      }
   }

   /**
    * {@inheritDoc}
    */
   public List<PropertyData> getChildPropertiesData(NodeData parent) throws RepositoryException, IllegalStateException
   {
      checkIfOpened();
      BasicDBObject query = new BasicDBObject(PARENT_ID, parent.getIdentifier()).append(IS_NODE, Boolean.FALSE);
      DBCursor cursor = collection.find(query).sort(new BasicDBObject(NAME, 1));
      List<PropertyData> result = null;
      try
      {
         result = new ArrayList<PropertyData>();
         while (cursor.hasNext())
         {
            result.add((PropertyData)itemData(parent.getQPath(), cursor.next(), false, parent.getACL()));
         }
      }
      catch (IOException e)
      {
         throw new RepositoryException(e);
      }
      finally
      {
         cursor.close();
      }
      return result;
   }

   private String escapeSpecialChars(String pattern)
   {
      char[] chars = pattern.toCharArray();
      StringBuilder sb = new StringBuilder(chars.length + 1);
      for (int i = 0; i < chars.length; i++)
      {
         switch (chars[i])
         {
            case '*' :
               sb.append(".*");
               break;
            case '(' :
            case ')' :
            case '[' :
            case ']' :
            case '{' :
            case '}' :
            case '-' :
            case '|' :
            case '?' :
            case '=' :
            case ':' :
            case '!' :
            case '<' :
            case '>' :
            case '\\' :
            case '+' :
            case '&' :
            case '#' :
            case '.' :
            case '^' :
            case '$' :
            case ',' :
               sb.append("\\");
            default :
               sb.append(chars[i]);
         }
      }
      return sb.toString();
   }

   /**
    * {@inheritDoc}
    */
   public List<PropertyData> getChildPropertiesData(NodeData parent, List<QPathEntryFilter> pattern)
      throws RepositoryException, IllegalStateException
   {
      checkIfOpened();
      if (pattern.isEmpty())
      {
         return Collections.emptyList();
      }
      BasicDBObject query = new BasicDBObject(PARENT_ID, parent.getIdentifier()).append(IS_NODE, Boolean.FALSE);
      List<BasicDBObject> orClause = new ArrayList<BasicDBObject>();
      for (QPathEntryFilter filter : pattern)
      {
         BasicDBObject clause = new BasicDBObject();
         QPathEntry entry = filter.getQPathEntry();
         String name = entry.getAsString();
         if (name.contains("*"))
         {
            Pattern pat = Pattern.compile(escapeSpecialChars(name));
            clause.append(NAME, pat);
         }
         else
         {
            clause.append(NAME, name);
         }
         orClause.add(clause);
      }
      query.append("$or", orClause);
      DBCursor cursor = collection.find(query).sort(new BasicDBObject(NAME, 1));
      List<PropertyData> result = null;
      try
      {
         result = new ArrayList<PropertyData>();
         while (cursor.hasNext())
         {
            result.add((PropertyData)itemData(parent.getQPath(), cursor.next(), false, parent.getACL()));
         }
      }
      catch (IOException e)
      {
         throw new RepositoryException(e);
      }
      finally
      {
         cursor.close();
      }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   public List<PropertyData> listChildPropertiesData(NodeData parent) throws RepositoryException, IllegalStateException
   {
      checkIfOpened();
      BasicDBObject query = new BasicDBObject(PARENT_ID, parent.getIdentifier()).append(IS_NODE, Boolean.FALSE);
      DBCursor cursor =
         collection.find(query, new BasicDBObject(VALUES, Boolean.FALSE)).sort(new BasicDBObject(NAME, 1));
      List<PropertyData> result = null;
      try
      {
         result = new ArrayList<PropertyData>();
         while (cursor.hasNext())
         {
            result.add((PropertyData)itemData(parent.getQPath(), cursor.next(), false, parent.getACL()));
         }
      }
      catch (IOException e)
      {
         throw new RepositoryException(e);
      }
      finally
      {
         cursor.close();
      }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   public List<PropertyData> getReferencesData(String nodeIdentifier) throws RepositoryException,
      IllegalStateException, UnsupportedOperationException
   {
      checkIfOpened();
      BasicDBObject query = new BasicDBObject(VALUES + "." + PROPERTY_TYPE_REFERENCE, nodeIdentifier);
      DBCursor cursor = collection.find(query);
      List<PropertyData> result = null;
      try
      {
         result = new ArrayList<PropertyData>();
         while (cursor.hasNext())
         {
            PropertyData propertyData = (PropertyData)itemData(null, cursor.next(), false, null);
            List<ValueData> values = propertyData.getValues();
            for (int i = 0, length = values.size(); i < length; i++)
            {
               ReferencePersistedValueData value = (ReferencePersistedValueData)values.get(i);
               if (nodeIdentifier.equals(value.toString()))
               {
                  result.add(propertyData);
               }
            }
         }
      }
      catch (IOException e)
      {
         throw new RepositoryException(e);
      }
      finally
      {
         cursor.close();
      }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   public boolean getChildNodesDataByPage(NodeData parent, int fromOrderNum, int offset, int pageSize,
      List<NodeData> childs) throws RepositoryException
   {
      checkIfOpened();
      BasicDBObject query = new BasicDBObject(PARENT_ID, parent.getIdentifier()).append(IS_NODE, Boolean.TRUE);
      DBCursor cursor = collection.find(query).sort(new BasicDBObject(ORDER_NUMBER, 1)).skip(offset).limit(pageSize);
      try
      {
         for (int i = 0; i < pageSize && cursor.hasNext(); i++)
         {
            childs.add((NodeData)itemData(parent.getQPath(), cursor.next(), true, parent.getACL()));
         }
      }
      catch (IOException e)
      {
         throw new RepositoryException(e);
      }
      finally
      {
         cursor.close();
      }
      return childs.size() >= pageSize;
   }

   /**
    * {@inheritDoc}
    */
   public void add(NodeData data) throws RepositoryException, UnsupportedOperationException, InvalidItemStateException,
      IllegalStateException
   {
      checkIfOpened();
      try
      {
         BasicDBObject node =
            new BasicDBObject(ID, data.getIdentifier())
               .append(PARENT_ID,
                  data.getParentIdentifier() == null ? Constants.ROOT_PARENT_UUID : data.getParentIdentifier())
               .append(NAME, data.getQPath().getName().getAsString()).append(INDEX, data.getQPath().getIndex())
               .append(IS_NODE, Boolean.TRUE).append(VERSION, data.getPersistedVersion())
               .append(ORDER_NUMBER, data.getOrderNumber());
         collection.insert(node);
         if (data.getParentIdentifier() != null)
         {
            BasicDBObject query = new BasicDBObject(ID, data.getParentIdentifier());
            DBObject parentNode = collection.findOne(query, new BasicDBObject(ID, Boolean.TRUE));
            if (parentNode == null)
               throw new JCRInvalidItemStateException("(added) Parent node not found " + data.getQPath().getAsString()
                  + " " + data.getIdentifier() + ". Probably was deleted by another session ", data.getIdentifier(),
                  ItemState.ADDED);

         }

         if (LOG.isDebugEnabled())
         {
            LOG.debug("Node added " + data.getQPath().getAsString() + ", " + data.getIdentifier() + ", "
               + data.getPrimaryTypeName().getAsString());
         }
      }
      catch (JCRInvalidItemStateException e)
      {
         throw e;
      }
      catch (DuplicateKey e)
      {
         CommandResult result = e.getCommandResult();
         if (result != null)
         {
            String msg = result.getString("err");
            if (msg != null && msg.contains(data.getIdentifier()))
            {
               throw new JCRInvalidItemStateException("Could not add node Path: " + data.getQPath().getAsString()
                  + ", ID: " + data.getIdentifier() + ", ParentID: " + data.getParentIdentifier()
                  + ". Cause >>>> Item already exists.", data.getIdentifier(), ItemState.ADDED, e);
            }
         }
         throw new ItemExistsException("Could not add node Path: " + data.getQPath().getAsString() + ", ID: "
            + data.getIdentifier() + ", ParentID: " + data.getParentIdentifier() + ". Cause >>>> Item already exists.",
            e);
      }
      catch (Exception e)
      {
         throw new RepositoryException("Could not add node Path: " + data.getQPath().getAsString() + ", ID: "
            + data.getIdentifier() + ", ParentID: " + data.getParentIdentifier() + ". Cause >>>> " + e.getMessage(), e);
      }

   }

   /**
    * {@inheritDoc}
    */
   public void add(PropertyData data, ChangedSizeHandler sizeHandler) throws RepositoryException,
      UnsupportedOperationException, InvalidItemStateException, IllegalStateException
   {
      checkIfOpened();
      try
      {
         String name = data.getQPath().getName().getAsString();
         BasicDBObject property =
            new BasicDBObject(ID, data.getIdentifier()).append(PARENT_ID, data.getParentIdentifier())
               .append(NAME, name).append(INDEX, data.getQPath().getIndex()).append(IS_NODE, Boolean.FALSE)
               .append(VERSION, data.getPersistedVersion()).append(TYPE, data.getType())
               .append(IS_MULTI_VALUED, data.isMultiValued());

         List<DBObject> dbValues = addValues(data.getIdentifier(), data, sizeHandler);
         property.append(VALUES, dbValues);
         collection.insert(property);
         if (MAIN_PROPERTIES.containsKey(name))
         {
            BasicDBObject query = new BasicDBObject(ID, data.getParentIdentifier());
            BasicDBObject update =
               new BasicDBObject("$set", new BasicDBObject(MAIN_PROPERTIES.get(name), getValues(data)));
            WriteResult result = collection.update(query, update);
            if (result.getN() <= 0)
            {
               throw new JCRInvalidItemStateException("(added) Parent node not found " + data.getQPath().getAsString()
                  + " " + data.getIdentifier() + ". Probably was deleted by another session ", data.getIdentifier(),
                  ItemState.ADDED);

            }

         }
         if (LOG.isDebugEnabled())
         {
            LOG.debug("Property added " + data.getQPath().getAsString() + ", " + data.getIdentifier()
               + (data.getValues() != null ? ", values count: " + data.getValues().size() : ", NULL data"));
         }
      }
      catch (IOException e)
      {
         if (LOG.isDebugEnabled())
         {
            LOG.error("Property add. IO error: " + e, e);
         }
         throw new RepositoryException("Error of Property Value add " + e, e);
      }
      catch (JCRInvalidItemStateException e)
      {
         throw e;
      }
      catch (DuplicateKey e)
      {
         CommandResult result = e.getCommandResult();
         if (result != null)
         {
            String msg = result.getString("err");
            if (msg != null && msg.contains(data.getIdentifier()))
            {
               throw new JCRInvalidItemStateException("Could not add property Path: " + data.getQPath().getAsString()
                  + ", ID: " + data.getIdentifier() + ", ParentID: " + data.getParentIdentifier()
                  + ". Cause >>>> Item already exists.", data.getIdentifier(), ItemState.ADDED, e);
            }
         }
         throw new ItemExistsException("Could not add property Path: " + data.getQPath().getAsString() + ", ID: "
            + data.getIdentifier() + ", ParentID: " + data.getParentIdentifier() + ". Cause >>>> Item already exists.",
            e);
      }
      catch (Exception e)
      {
         throw new RepositoryException("Could not add property Path: " + data.getQPath().getAsString() + ", ID: "
            + data.getIdentifier() + ", ParentID: " + data.getParentIdentifier() + ". Cause >>>> " + e.getMessage(), e);
      }
   }

   private List<DBObject> addValues(String cid, final PropertyData data, ChangedSizeHandler sizeHandler)
      throws IOException, RepositoryException
   {
      List<ValueData> vdata = data.getValues();
      List<DBObject> dbValues = new ArrayList<DBObject>();
      for (int i = 0; i < vdata.size(); i++)
      {
         ValueData vd = vdata.get(i);
         final ValueIOChannel channel = valueStorageProvider.getApplicableChannel(data, i);
         InputStream stream;
         int streamLength;
         String storageId;
         if (channel == null)
         {
            // prepare write of Value in database
            if (vd.isByteArray())
            {
               byte[] dataBytes = vd.getAsByteArray();
               stream = new ByteArrayInputStream(dataBytes);
               streamLength = dataBytes.length;
            }
            else
            {
               StreamPersistedValueData streamData = (StreamPersistedValueData)vd;

               SwapFile swapFile =
                  SwapFile.get(spoolConfig.tempDirectory, cid + i + "." + data.getPersistedVersion(),
                     spoolConfig.fileCleaner);
               try
               {
                  long vlen = WRITE_VALUE_HELPER.writeStreamedValue(swapFile, streamData);
                  if (vlen <= Integer.MAX_VALUE)
                  {
                     streamLength = (int)vlen;
                  }
                  else
                  {
                     throw new RepositoryException("Value data large of allowed by JDBC (Integer.MAX_VALUE) " + vlen
                        + ". Property " + data.getQPath().getAsString());
                  }
               }
               finally
               {
                  swapFile.spoolDone();
               }

               stream = streamData.getAsStream();
            }
            storageId = null;
            sizeHandler.accumulateNewSize(streamLength);
         }
         else
         {
            // write Value in external VS
            channel.write(data.getIdentifier(), vd, sizeHandler);
            valueChanges.add(channel);
            storageId = channel.getStorageId();
            stream = null;
            streamLength = 0;
         }

         BasicDBObject value = new BasicDBObject(INDEX, i);
         if (stream == null)
         {
            value.append(VALUE_STORAGE, storageId);
            final int orderNumber = i;
            Callable<Void> task = new Callable<Void>()
            {
               public Void call() throws Exception
               {
                  try
                  {
                     BasicDBObject query = new BasicDBObject(ID, data.getIdentifier());
                     BasicDBObject update =
                        new BasicDBObject("$set", new BasicDBObject(VALUES + "." + orderNumber + "." + SIZE,
                           channel.getValueSize(data.getIdentifier(), orderNumber)));
                     WriteResult result = collection.update(query, update);
                     if (result.getN() <= 0)
                     {
                        throw new RepositoryException("Could not update the size of the property"
                           + data.getQPath().getAsString() + " " + data.getIdentifier() + ". Order number: "
                           + orderNumber);
                     }
                  }
                  catch (RepositoryException e)
                  {
                     throw e;
                  }
                  catch (Exception e)
                  {
                     throw new RepositoryException(
                        "Could not update the size of the property" + data.getQPath().getAsString() + " "
                           + data.getIdentifier() + ". Order number: " + orderNumber, e);
                  }
                  return null;
               }
            };
            if (updateSizeTasks == null)
            {
               updateSizeTasks = new ArrayList<Callable<Void>>();
            }
            updateSizeTasks.add(task);
         }
         else
         {
            addValue(value, data.getType(), vd, stream, streamLength);
         }
         dbValues.add(value);
      }
      return dbValues;
   }

   private void addValue(BasicDBObject value, int type, ValueData vdata, InputStream stream) throws IOException,
      RepositoryException
   {
      value.append(PROPERTY_TYPES.get(type), getValue(type, vdata, stream));
   }

   private static List<Object> getValues(PropertyData data) throws IOException, RepositoryException
   {
      List<ValueData> vdata = data.getValues();
      List<Object> values = new ArrayList<Object>(vdata.size());
      for (int i = 0; i < vdata.size(); i++)
      {
         ValueData vd = vdata.get(i);
         values.add(getValue(data.getType(), vd, null));
      }
      return values;
   }

   private static Object getValue(int type, ValueData vdata, InputStream stream) throws IOException,
      RepositoryException
   {
      switch (type)
      {
         case PropertyType.BINARY :
         case PropertyType.UNDEFINED :
            return vdata.getAsByteArray();

         case PropertyType.BOOLEAN :
            return ValueDataUtil.getBoolean(vdata);

         case PropertyType.DOUBLE :
            return ValueDataUtil.getDouble(vdata);

         case PropertyType.LONG :
            return ValueDataUtil.getLong(vdata);

         case PropertyType.DATE :
         case PropertyType.NAME :
         case PropertyType.PATH :
         case ExtendedPropertyType.PERMISSION :
         case PropertyType.REFERENCE :
         case PropertyType.STRING :
            return ValueDataUtil.getString(vdata);

         default :
            throw new IllegalStateException("Unknown property type " + type);
      }
   }

   private void addValue(BasicDBObject value, int type, ValueData vdata, InputStream stream, int streamLength)
      throws IOException, RepositoryException
   {
      value.append(SIZE, (long)streamLength);
      addValue(value, type, vdata, stream);
   }

   /**
    * {@inheritDoc}
    */
   public void update(NodeData data) throws RepositoryException, UnsupportedOperationException,
      InvalidItemStateException, IllegalStateException
   {
      checkIfOpened();
      try
      {
         BasicDBObject target = new BasicDBObject(ID, data.getIdentifier());
         BasicDBObject update =
            new BasicDBObject(VERSION, data.getPersistedVersion()).append(INDEX, data.getQPath().getIndex()).append(
               ORDER_NUMBER, data.getOrderNumber());
         WriteResult result = collection.update(target, new BasicDBObject("$set", update), false, false);
         // order numb update
         if (result.getN() <= 0)
         {
            throw new JCRInvalidItemStateException("(update) Node not found " + data.getQPath().getAsString() + " "
               + data.getIdentifier() + ". Probably was deleted by another session ", data.getIdentifier(),
               ItemState.UPDATED);
         }

         if (LOG.isDebugEnabled())
         {
            LOG.debug("Node updated " + data.getQPath().getAsString() + ", " + data.getIdentifier() + ", "
               + data.getPrimaryTypeName().getAsString());
         }
      }
      catch (JCRInvalidItemStateException e)
      {
         throw e;
      }
      catch (Exception e)
      {
         throw new RepositoryException("Could not update node Path: " + data.getQPath().getAsString() + ", ID: "
            + data.getIdentifier() + ", ParentID: " + data.getParentIdentifier() + ". Cause >>>> " + e.getMessage(), e);
      }
   }

   /**
    * {@inheritDoc}
    */
   public void update(PropertyData data, ChangedSizeHandler sizeHandler) throws RepositoryException,
      UnsupportedOperationException, InvalidItemStateException, IllegalStateException
   {
      checkIfOpened();
      try
      {
         String cid = data.getIdentifier();
         // do Values update: delete all and add all
         deleteValues(data, sizeHandler);
         BasicDBObject target = new BasicDBObject(ID, cid);
         BasicDBObject update = new BasicDBObject(VERSION, data.getPersistedVersion()).append(TYPE, data.getType());
         List<DBObject> dbValues = addValues(data.getIdentifier(), data, sizeHandler);
         update.append(VALUES, dbValues);
         WriteResult result = collection.update(target, new BasicDBObject("$set", update), false, false);
         // update type
         if (result.getN() <= 0)
         {
            throw new JCRInvalidItemStateException("(update) Property not found " + data.getQPath().getAsString() + " "
               + data.getIdentifier() + ". Probably was deleted by another session ", data.getIdentifier(),
               ItemState.UPDATED);
         }
         String name = data.getQPath().getName().getAsString();
         if (MAIN_PROPERTIES.containsKey(name))
         {
            BasicDBObject query = new BasicDBObject(ID, data.getParentIdentifier());
            BasicDBObject updateNode =
               new BasicDBObject("$set", new BasicDBObject(MAIN_PROPERTIES.get(name), getValues(data)));
            WriteResult resultUpdate = collection.update(query, updateNode);
            if (resultUpdate.getN() <= 0)
            {
               throw new JCRInvalidItemStateException("(update) Parent node not found " + data.getQPath().getAsString()
                  + " " + data.getIdentifier() + ". Probably was deleted by another session ", data.getIdentifier(),
                  ItemState.UPDATED);

            }
         }
         if (LOG.isDebugEnabled())
         {
            LOG.debug("Property updated " + data.getQPath().getAsString() + ", " + data.getIdentifier()
               + (data.getValues() != null ? ", values count: " + data.getValues().size() : ", NULL data"));
         }
      }
      catch (IOException e)
      {
         if (LOG.isDebugEnabled())
         {
            LOG.error("Property update. IO error: " + e, e);
         }
         throw new RepositoryException("Error of Property Value update " + e, e);
      }
      catch (JCRInvalidItemStateException e)
      {
         throw e;
      }
      catch (Exception e)
      {
         throw new RepositoryException("Could not update property Path: " + data.getQPath().getAsString() + ", ID: "
            + data.getIdentifier() + ", ParentID: " + data.getParentIdentifier() + ". Cause >>>> " + e.getMessage(), e);
      }
   }

   /**
    * {@inheritDoc}
    */
   public void rename(NodeData data) throws RepositoryException, UnsupportedOperationException,
      InvalidItemStateException, IllegalStateException
   {
      checkIfOpened();
      try
      {
         BasicDBObject target = new BasicDBObject(ID, data.getIdentifier());
         BasicDBObject update =
            new BasicDBObject(PARENT_ID, data.getParentIdentifier() == null ? Constants.ROOT_PARENT_UUID
               : data.getParentIdentifier()).append(NAME, data.getQPath().getName().getAsString())
               .append(VERSION, data.getPersistedVersion()).append(INDEX, data.getQPath().getIndex())
               .append(ORDER_NUMBER, data.getOrderNumber());
         WriteResult result = collection.update(target, new BasicDBObject("$set", update), false, false);

         if (result.getN() <= 0)
         {
            throw new JCRInvalidItemStateException("(rename) Node not found " + data.getQPath().getAsString() + " "
               + data.getIdentifier() + ". Probably was deleted by another session ", data.getIdentifier(),
               ItemState.RENAMED);
         }
      }
      catch (JCRInvalidItemStateException e)
      {
         throw e;
      }
      catch (Exception e)
      {
         throw new RepositoryException("Could not rename node Path: " + data.getQPath().getAsString() + ", ID: "
            + data.getIdentifier() + ", ParentID: " + data.getParentIdentifier() + ". Cause >>>> " + e.getMessage(), e);
      }
   }

   /**
    * {@inheritDoc}
    */
   public void delete(NodeData data) throws RepositoryException, UnsupportedOperationException,
      InvalidItemStateException, IllegalStateException
   {
      checkIfOpened();
      try
      {
         BasicDBObject node = new BasicDBObject(ID, data.getIdentifier());
         WriteResult result = collection.remove(node);
         if (result.getN() <= 0)
         {
            throw new JCRInvalidItemStateException("(delete) Node not found " + data.getQPath().getAsString() + " "
               + data.getIdentifier() + ". Probably was deleted by another session ", data.getIdentifier(),
               ItemState.DELETED);
         }
         if (LOG.isDebugEnabled())
         {
            LOG.debug("Node deleted " + data.getQPath().getAsString() + ", " + data.getIdentifier() + ", "
               + (data).getPrimaryTypeName().getAsString());
         }
      }
      catch (JCRInvalidItemStateException e)
      {
         throw e;
      }
      catch (Exception e)
      {
         throw new RepositoryException("Could not delete node Path: " + data.getQPath().getAsString() + ", ID: "
            + data.getIdentifier() + ", ParentID: " + data.getParentIdentifier() + ". Cause >>>> " + e.getMessage(), e);
      }

   }

   /**
    * {@inheritDoc}
    */
   public void delete(PropertyData data, ChangedSizeHandler sizeHandler) throws RepositoryException,
      UnsupportedOperationException, InvalidItemStateException, IllegalStateException
   {
      checkIfOpened();
      String cid = data.getIdentifier();
      try
      {
         deleteValues(data, sizeHandler);
         BasicDBObject node = new BasicDBObject(ID, cid);
         WriteResult result = collection.remove(node);
         if (result.getN() <= 0)
         {
            throw new JCRInvalidItemStateException("(delete) Property not found " + data.getQPath().getAsString() + " "
               + data.getIdentifier() + ". Probably was deleted by another session ", data.getIdentifier(),
               ItemState.DELETED);
         }
         String name = data.getQPath().getName().getAsString();
         if (MAIN_PROPERTIES.containsKey(name))
         {
            BasicDBObject query = new BasicDBObject(ID, data.getParentIdentifier());
            BasicDBObject updateNode =
               new BasicDBObject("$unset", new BasicDBObject(MAIN_PROPERTIES.get(name), Boolean.TRUE));
            result = collection.update(query, updateNode);
            if (result.getN() <= 0)
            {
               throw new JCRInvalidItemStateException("(delete) Parent node not found " + data.getQPath().getAsString()
                  + " " + data.getIdentifier() + ". Probably was deleted by another session ", data.getIdentifier(),
                  ItemState.DELETED);
            }
         }
         if (LOG.isDebugEnabled())
         {
            LOG.debug("Property deleted " + data.getQPath().getAsString() + ", " + data.getIdentifier()
               + ((data).getValues() != null ? ", values count: " + (data).getValues().size() : ", NULL data"));
         }
      }
      catch (IOException e)
      {
         if (LOG.isDebugEnabled())
         {
            LOG.error("Property remove. IO error: " + e, e);
         }
         throw new RepositoryException("Error of Property Value delete " + e, e);
      }
      catch (JCRInvalidItemStateException e)
      {
         throw e;
      }
      catch (Exception e)
      {
         throw new RepositoryException("Could not delete property Path: " + data.getQPath().getAsString() + ", ID: "
            + data.getIdentifier() + ", ParentID: " + data.getParentIdentifier() + ". Cause >>>> " + e.getMessage(), e);
      }

   }

   /**
    * Delete Property Values.
    * 
    * @param pdata
    *          PropertyData
    * @param sizeHandler
    *          accumulates changed size
    * @throws IOException
    *           i/O error
    * @throws RepositoryException 
    * @throws InvalidItemStateException 
    */
   private void deleteValues(PropertyData pdata, ChangedSizeHandler sizeHandler) throws IOException,
      RepositoryException, InvalidItemStateException
   {
      String cid = pdata.getIdentifier();
      BasicDBObject query = new BasicDBObject(ID, cid);
      DBObject result = collection.findOne(query, new BasicDBObject(VALUES, Boolean.TRUE));
      if (result == null)
         return;
      Set<String> storages = new HashSet<String>();
      @SuppressWarnings("unchecked")
      List<DBObject> valueRecords = (List<DBObject>)result.get(VALUES);
      for (DBObject valueRecord : valueRecords)
      {
         String storageId = (String)valueRecord.get(VALUE_STORAGE);
         if (storageId != null)
         {
            storages.add(storageId);
         }
         else
         {
            sizeHandler.accumulatePrevSize((Long)valueRecord.get(SIZE));
         }
      }

      // delete all values in value storage
      for (String storageId : storages)
      {
         final ValueIOChannel channel = valueStorageProvider.getChannel(storageId);
         try
         {
            sizeHandler.accumulatePrevSize(channel.getValueSize(cid));

            channel.delete(cid);
            valueChanges.add(channel);
         }
         finally
         {
            channel.close();
         }
      }
   }

   /**
    * {@inheritDoc}
    */
   public void prepare() throws IllegalStateException, RepositoryException
   {
      try
      {
         for (ValueIOChannel vo : valueChanges)
         {
            vo.prepare();
         }
      }
      catch (IOException e)
      {
         throw new RepositoryException(e);
      }
      if (updateSizeTasks != null)
      {
         for (Callable<Void> task : updateSizeTasks)
         {
            try
            {
               task.call();
            }
            catch (Exception e)
            {
               LOG.warn(e.getMessage());
               LOG.debug(e);
            }
         }
      }
   }

   private void commitTransaction()
   {
      if (!autoCommit)
         db.command("commitTransaction");
   }

   /**
    * {@inheritDoc}
    */
   public void commit() throws IllegalStateException, RepositoryException
   {
      checkIfOpened();
      try
      {

         if (!readOnly)
         {
            try
            {
               for (ValueIOChannel vo : valueChanges)
               {
                  vo.twoPhaseCommit();
               }
            }
            catch (IOException e)
            {
               throw new RepositoryException(e);
            }
            finally
            {
               valueChanges.clear();
            }
            commitTransaction();
         }
      }
      finally
      {
         close();
      }
   }

   /**
    * {@inheritDoc}
    */
   public void rollback() throws IllegalStateException, RepositoryException
   {
      checkIfOpened();
      try
      {

         if (!readOnly)
         {
            try
            {
               rollbackTransaction();
            }
            finally
            {
               // rollback from the end
               IOException e = null;
               for (int p = valueChanges.size() - 1; p >= 0; p--)
               {
                  try
                  {
                     valueChanges.get(p).rollback();
                  }
                  catch (IOException e1)
                  {
                     if (e == null)
                     {
                        e = e1;
                     }
                     else
                     {
                        LOG.error("Could not rollback value change", e1);
                     }
                  }
               }
               if (e != null)
               {
                  throw e;
               }
            }
         }
      }
      catch (IOException e)
      {
         throw new RepositoryException(e);
      }
      finally
      {
         valueChanges.clear();
         close();
      }
   }

   private void rollbackTransaction()
   {
      if (!autoCommit)
         db.command("rollbackTransaction");
   }

   /**
    * {@inheritDoc}
    */
   public void close() throws IllegalStateException, RepositoryException
   {
      db.requestDone();
      if (readOnly)
         db.setReadOnly(false);
      closed = true;
   }

   /**
    * {@inheritDoc}
    */
   public boolean isOpened()
   {
      return !closed;
   }

   /**
    * @throws IllegalStateException
    *           if connection is closed.
    */
   protected void checkIfOpened() throws IllegalStateException
   {
      if (!isOpened())
      {
         throw new IllegalStateException("Connection is already closed");
      }
   }

   /**
    * {@inheritDoc}
    */
   public List<ACLHolder> getACLHolders() throws RepositoryException, IllegalStateException,
      UnsupportedOperationException
   {
      checkIfOpened();
      String owner = EXO_OWNER;
      String permissions = EXO_PERMISSIONS;
      BasicDBObject query = new BasicDBObject(IS_NODE, Boolean.TRUE);
      List<BasicDBObject> orClause = new ArrayList<BasicDBObject>();
      orClause.add(new BasicDBObject(owner, new BasicDBObject("$exists", Boolean.TRUE)));
      orClause.add(new BasicDBObject(permissions, new BasicDBObject("$exists", Boolean.TRUE)));
      query.append("$or", orClause);
      DBCursor cursor =
         collection.find(query,
            new BasicDBObject(ID, Boolean.TRUE).append(owner, Boolean.TRUE).append(permissions, Boolean.TRUE));
      List<ACLHolder> result = null;
      try
      {
         result = new ArrayList<ACLHolder>();
         while (cursor.hasNext())
         {
            DBObject node = cursor.next();
            ACLHolder holder = new ACLHolder((String)node.get(ID));
            holder.setOwner(node.containsField(owner));
            holder.setPermissions(node.containsField(permissions));
            result.add(holder);
         }
      }
      finally
      {
         cursor.close();
      }
      return result;
   }

   /**
    * {@inheritDoc}
    */
   public long getNodesCount() throws RepositoryException
   {
      checkIfOpened();
      return collection.count(new BasicDBObject(IS_NODE, Boolean.TRUE));
   }

   /**
    * {@inheritDoc}
    */
   public long getWorkspaceDataSize() throws RepositoryException
   {
      checkIfOpened();
      MapReduceCommand cmd =
         new MapReduceCommand(collection, MAP, REDUCE, null, MapReduceCommand.OutputType.INLINE, new BasicDBObject(
            IS_NODE, Boolean.FALSE));
      MapReduceOutput out = collection.mapReduce(cmd);
      return ((Double)out.results().iterator().next().get("value")).longValue();
   }

   /**
    * {@inheritDoc}
    */
   public long getNodeDataSize(String nodeIdentifier) throws RepositoryException
   {
      checkIfOpened();
      MapReduceCommand cmd =
         new MapReduceCommand(collection, MAP, REDUCE, null, MapReduceCommand.OutputType.INLINE, new BasicDBObject(
            PARENT_ID, nodeIdentifier).append(IS_NODE, Boolean.FALSE));
      MapReduceOutput out = collection.mapReduce(cmd);
      return ((Double)out.results().iterator().next().get("value")).longValue();
   }

   /**
    * Deletes all the properties related to locks
    */
   public void deleteLockProperties()
   {
      BasicDBObject query = new BasicDBObject(IS_NODE, Boolean.FALSE);
      List<BasicDBObject> orClause = new ArrayList<BasicDBObject>();
      orClause.add(new BasicDBObject(NAME, "[http://www.jcp.org/jcr/1.0]lockIsDeep"));
      orClause.add(new BasicDBObject(NAME, "[http://www.jcp.org/jcr/1.0]lockOwner"));
      query.append("$or", orClause);
      collection.remove(query);
   }

}
