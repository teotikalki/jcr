/*
 * Copyright (C) 2003-2010 eXo Platform SAS.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see<http://www.gnu.org/licenses/>.
 */
package org.exoplatform.services.jcr.impl.util.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by The eXo Platform SAS.
 * 
 * <br/>Date: 
 *
 * @author <a href="karpenko.sergiy@gmail.com">Karpenko Sergiy</a> 
 * @version $Id: OracleDBCleaner.java 111 2008-11-11 11:11:11Z serg $
 */
public class OracleDBCleaner extends DBCleaner
{

   /**
    * Constructor.
    */
   public OracleDBCleaner(String containerName, Connection connection, InputStream inputStream, boolean isMultiDB)
      throws IOException
   {
      super(containerName, connection, inputStream, isMultiDB);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   protected boolean isTableExists(Connection conn, String tableName) throws SQLException
   {
      Statement st = null;
      try
      {
         st = conn.createStatement();
         st.executeUpdate("SELECT 1 FROM " + tableName);
         return true;
      }
      catch (SQLException e)
      {
         // check: ORA-00942: table or view does not exist
         if (e.getMessage().indexOf("ORA-00942") >= 0)
            return false;
         throw e;
      }
      finally
      {
         if (st != null)
         {
            try
            {
               st.close();
            }
            catch (SQLException e)
            {
               LOG.error("Can't close the Statement: " + e);
            }
         }
      }
   }
}