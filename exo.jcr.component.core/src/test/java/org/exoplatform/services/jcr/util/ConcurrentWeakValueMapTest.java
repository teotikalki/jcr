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
package org.exoplatform.services.jcr.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class ConcurrentWeakValueMapTest
{
   private ConcurrentWeakValueMap<Object, Object> map;

   @Before
   public void init()
   {
      this.map = new ConcurrentWeakValueMap<Object, Object>();
   }

   @After
   public void clean()
   {
      map.clear();
   }

   @Test
   public void testPutNGet()
   {
      assertTrue(map.isEmpty());
      Object key = new Object();
      map.put(key, new String("a"));
      assertEquals(1, map.size());
      assertNotNull(map.get(key));
      assertEquals(new String("a"), map.get(key));
      map.put(key, new String("b"));
      assertEquals(1, map.size());
      assertNotNull(map.get(key));
      assertEquals(new String("b"), map.get(key));
      callGCNSleep();
      assertNull(map.get(key));
      assertTrue(map.isEmpty());
      Object value = new String("a");
      map.put(key, value);
      assertEquals(1, map.size());
      assertNotNull(map.get(key));
      assertEquals(new String("a"), map.get(key));
      callGCNSleep();
      assertEquals(1, map.size());
      assertNotNull(map.get(key));
      assertEquals(new String("a"), map.get(key));
      value = null;
      callGCNSleep();
      assertNull(map.get(key));
      assertTrue(map.isEmpty());
      value = new String("a");
      assertNull(map.putIfAbsent(key, value));
      assertEquals(1, map.size());
      assertNotNull(map.get(key));
      assertEquals(new String("a"), map.get(key));
      callGCNSleep();
      assertEquals(1, map.size());
      assertNotNull(map.get(key));
      assertEquals(new String("a"), map.get(key));
      assertEquals(new String("a"), map.putIfAbsent(key, new String("b")));
      assertEquals(1, map.size());
      assertNotNull(map.get(key));
      assertEquals(new String("a"), map.get(key));
      map.remove(key, value);
      assertNull(map.get(key));
      assertTrue(map.isEmpty());
      assertNull(map.putIfAbsent(key, value));
      assertTrue(map.putIfAbsent(key, new String("a")) == value);
      assertTrue(map.putIfAbsent(key, new String("a")) == value);
      assertTrue(map.putIfAbsent(key, new String("a")) == value);
      assertEquals(1, map.size());
      assertNotNull(map.get(key));
      assertEquals(new String("a"), map.get(key));
      assertEquals(new String("a"), map.putIfAbsent(key, new String("b")));
      value = null;
      callGCNSleep();
      assertNull(map.putIfAbsent(key, new String("b")));
      assertEquals(1, map.size());
      assertNotNull(map.get(key));
      assertEquals(new String("b"), map.get(key));
      assertEquals(new String("b"), map.putIfAbsent(key, new String("a")));
   }

   @Test
   public void testRemove()
   {
      assertTrue(map.isEmpty());
      Object key = new Object();
      map.put(key, new String("a"));
      assertEquals(1, map.size());
      assertNotNull(map.get(key));
      assertEquals(new String("a"), map.remove(key));
      assertTrue(map.isEmpty());
      assertNull(map.remove(key));
      map.put(key, new String("a"));
      assertEquals(1, map.size());
      assertNotNull(map.get(key));
      callGCNSleep();
      assertNull(map.remove(key));
      assertTrue(map.isEmpty());
      Object value = new String("a");
      map.put(key, value);
      assertTrue(map.remove(key, null));
      assertTrue(map.isEmpty());
      map.put(key, value);
      assertEquals(1, map.size());
      assertFalse(map.remove(key, new String("b")));
      assertEquals(1, map.size());
      assertTrue(map.remove(key, new String("a")));
      assertTrue(map.isEmpty());
      map.put(key, value);
      callGCNSleep();
      assertTrue(map.remove(key, new String("a")));
      assertTrue(map.isEmpty());
      map.put(key, value);
      value = null;
      callGCNSleep();
      assertFalse(map.remove(key, new String("a")));
      assertTrue(map.isEmpty());
   }

   @Test
   public void testSize()
   {
      assertTrue(map.isEmpty());
      Object key = new Object();
      Object value = new String("a");
      map.put(key, value);
      assertEquals(1, map.size());
      callGCNSleep();
      assertEquals(1, map.size());
      value = null;
      callGCNSleep();
      assertTrue(map.isEmpty());
   }

   @Test
   public void testEntrySet()
   {
      assertTrue(map.isEmpty());
      Object key1 = new Object();
      Object value1 = new String("1");
      map.put(key1, value1);
      Object key2 = new Object();
      Object value2 = new String("2");
      map.put(key2, value2);
      Object key3 = new Object();
      Object value3 = new String("3");
      map.put(key3, value3);
      int count = 0;
      for (Entry<Object, Object> entry : map.entrySet())
      {
         assertNotNull(entry.getKey());
         assertNotNull(entry.getValue());
         Object key = entry.getKey();
         if (key == key1)
         {
            assertEquals(value1, entry.getValue());
         }
         else if (key == key2)
         {
            assertEquals(value2, entry.getValue());
         }
         else if (key == key3)
         {
            assertEquals(value3, entry.getValue());
         }
         else
         {
            fail("Should not occur");
         }
         count++;
      }
      assertEquals(3, count);
      assertEquals(3, map.entrySet().size());
      Iterator<Entry<Object, Object>> it = map.entrySet().iterator();
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      Entry<Object, Object> currentEntry = it.next();
      assertNotNull(currentEntry);
      assertNotNull(currentEntry.getKey());
      assertNotNull(currentEntry.getValue());
      Entry<Object, Object> nextEntry = it.next();
      assertNotNull(nextEntry);
      assertFalse(nextEntry == currentEntry);
      assertNotNull(nextEntry.getKey());
      assertFalse(nextEntry.getKey() == currentEntry.getKey());
      assertNotNull(nextEntry.getValue());
      assertFalse(nextEntry.getValue() == currentEntry.getValue());
      currentEntry = nextEntry;
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      nextEntry = it.next();
      assertNotNull(nextEntry);
      assertFalse(nextEntry == currentEntry);
      assertNotNull(nextEntry.getKey());
      assertFalse(nextEntry.getKey() == currentEntry.getKey());
      assertNotNull(nextEntry.getValue());
      assertFalse(nextEntry.getValue() == currentEntry.getValue());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      try
      {
         it.next();
         fail("Should throw a NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // expected
      }
      it = null;
      callGCNSleep();
      count = 0;
      for (Entry<Object, Object> entry : map.entrySet())
      {
         assertNotNull(entry.getKey());
         assertNotNull(entry.getValue());
         Object key = entry.getKey();
         if (key == key1)
         {
            assertEquals(value1, entry.getValue());
         }
         else if (key == key2)
         {
            assertEquals(value2, entry.getValue());
         }
         else if (key == key3)
         {
            assertEquals(value3, entry.getValue());
         }
         else
         {
            fail("Should not occur");
         }
         count++;
      }
      assertEquals(3, count);
      assertEquals(3, map.entrySet().size());
      it = map.entrySet().iterator();
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      currentEntry = it.next();
      assertNotNull(currentEntry);
      assertNotNull(currentEntry.getKey());
      assertNotNull(currentEntry.getValue());
      nextEntry = it.next();
      assertNotNull(nextEntry);
      assertFalse(nextEntry == currentEntry);
      assertNotNull(nextEntry.getKey());
      assertFalse(nextEntry.getKey() == currentEntry.getKey());
      assertNotNull(nextEntry.getValue());
      assertFalse(nextEntry.getValue() == currentEntry.getValue());
      currentEntry = nextEntry;
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      nextEntry = it.next();
      assertNotNull(nextEntry);
      assertFalse(nextEntry == currentEntry);
      assertNotNull(nextEntry.getKey());
      assertFalse(nextEntry.getKey() == currentEntry.getKey());
      assertNotNull(nextEntry.getValue());
      assertFalse(nextEntry.getValue() == currentEntry.getValue());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      try
      {
         it.next();
         fail("Should throw a NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // expected
      }
      it = null;
      value2 = null;
      callGCNSleep();
      count = 0;
      for (Entry<Object, Object> entry : map.entrySet())
      {
         assertNotNull(entry.getKey());
         assertNotNull(entry.getValue());
         Object key = entry.getKey();
         if (key == key1)
         {
            assertEquals(value1, entry.getValue());
         }
         else if (key == key3)
         {
            assertEquals(value3, entry.getValue());
         }
         else
         {
            fail("Should not occur");
         }
         count++;
      }
      assertEquals(2, count);
      assertEquals(2, map.entrySet().size());
      it = map.entrySet().iterator();
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      currentEntry = it.next();
      assertNotNull(currentEntry);
      assertNotNull(currentEntry.getKey());
      assertNotNull(currentEntry.getValue());
      nextEntry = it.next();
      assertNotNull(nextEntry);
      assertFalse(nextEntry == currentEntry);
      assertNotNull(nextEntry.getKey());
      assertFalse(nextEntry.getKey() == currentEntry.getKey());
      assertNotNull(nextEntry.getValue());
      assertFalse(nextEntry.getValue() == currentEntry.getValue());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      try
      {
         it.next();
         fail("Should throw a NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // expected
      }
      value2 = new String("2");
      map.put(key2, value2);
      it = map.entrySet().iterator();
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      currentEntry = it.next();
      assertNotNull(currentEntry);
      assertNotNull(currentEntry.getKey());
      assertNotNull(currentEntry.getValue());
      assertTrue(it.hasNext());
      // Ensure that the next entry won't be null even after a gc call
      value2 = null;
      callGCNSleep();
      nextEntry = it.next();
      assertNotNull(nextEntry);
      assertFalse(nextEntry == currentEntry);
      assertNotNull(nextEntry.getKey());
      assertFalse(nextEntry.getKey() == currentEntry.getKey());
      assertFalse(nextEntry.getValue() == currentEntry.getValue());
      currentEntry = nextEntry;
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      assertTrue(it.hasNext());
      nextEntry = it.next();
      assertNotNull(nextEntry);
      assertFalse(nextEntry == currentEntry);
      assertNotNull(nextEntry.getKey());
      assertFalse(nextEntry.getKey() == currentEntry.getKey());
      assertFalse(nextEntry.getValue() == currentEntry.getValue());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      assertFalse(it.hasNext());
      try
      {
         it.next();
         fail("Should throw a NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // expected
      }
   }

   @Test
   public void testContainsKey()
   {
      assertTrue(map.isEmpty());
      Object key = new String("key");
      Object value = new String("value");
      map.put(key, value);
      assertTrue(map.containsKey(key));
      assertTrue(map.containsKey(new String("key")));
      value = null;
      callGCNSleep();
      assertFalse(map.containsKey(key));
      assertFalse(map.containsKey(new String("key")));
   }

   @Test
   public void testReplace()
   {
      assertTrue(map.isEmpty());
      Object key = new Object();
      Object value = new String("a");
      assertNull(map.replace(key, value));
      assertTrue(map.isEmpty());
      map.put(key, value);
      assertTrue(map.replace(key, new String("b")) == value);
      assertEquals(new String("b"), map.get(key));
      assertEquals(new String("b"), map.replace(key, value));
      assertTrue(map.get(key) == value);
      assertTrue(map.replace(key, value) == value);
      callGCNSleep();
      assertTrue(map.replace(key, new String("b")) == value);
      assertEquals(new String("b"), map.get(key));
      assertEquals(new String("b"), map.replace(key, value));
      assertTrue(map.get(key) == value);
      assertTrue(map.replace(key, value) == value);
      value = null;
      callGCNSleep();
      value = new String("a");
      assertNull(map.replace(key, value));
      assertTrue(map.isEmpty());
      map.put(key, value);
      assertTrue(map.replace(key, new String("b")) == value);
      assertEquals(new String("b"), map.get(key));
      assertEquals(new String("b"), map.replace(key, value));
      assertTrue(map.get(key) == value);
      assertTrue(map.replace(key, value) == value);
      map.remove(key, value);
      assertFalse(map.replace(key, value, value));
      map.put(key, value);
      assertFalse(map.replace(key, new String("b"), new String("c")));
      assertTrue(map.get(key) == value);
      assertTrue(map.replace(key, value, value));
      assertTrue(map.get(key) == value);
      assertTrue(map.replace(key, new String("a"), value));
      assertTrue(map.get(key) == value);
      callGCNSleep();
      assertFalse(map.replace(key, new String("b"), new String("c")));
      assertTrue(map.get(key) == value);
      assertTrue(map.replace(key, value, value));
      assertTrue(map.get(key) == value);
      assertTrue(map.replace(key, new String("a"), value));
      assertTrue(map.get(key) == value);
      map.remove(key, value);
      assertFalse(map.replace(key, value, value));
      map.put(key, value);
      value = null;
      callGCNSleep();
      value = new String("a");
      assertFalse(map.replace(key, value, value));
      assertFalse(map.replace(key, new String("b"), new String("c")));
   }

   @Test
   public void testMultiThreading() throws Exception
   {
      final int totalElement = 100;
      final int totalTimes = 20;
      int reader = 20;
      int writer = 10;
      int remover = 5;
      int cleaner = 1;
      final CountDownLatch startSignalWriter = new CountDownLatch(1);
      final CountDownLatch startSignalOthers = new CountDownLatch(1);
      final CountDownLatch doneSignal = new CountDownLatch(reader + writer + remover);
      final List<Exception> errors = Collections.synchronizedList(new ArrayList<Exception>());
      for (int i = 0; i < writer; i++)
      {
         final int index = i;
         Thread thread = new Thread()
         {
            public void run()
            {
               try
               {
                  startSignalWriter.await();
                  for (int j = 0; j < totalTimes; j++)
                  {
                     for (int i = 0; i < totalElement; i++)
                     {
                        map.put(new MyKey("key" + i), "value" + i);
                     }
                     if (index == 0 && j == 0)
                     {
                        // The cache is full, we can launch the others
                        startSignalOthers.countDown();
                     }
                     sleep(50);
                  }
               }
               catch (Exception e)
               {
                  errors.add(e);
               }
               finally
               {
                  doneSignal.countDown();
               }
            }
         };
         thread.start();
      }
      startSignalWriter.countDown();
      for (int i = 0; i < reader; i++)
      {
         Thread thread = new Thread()
         {
            public void run()
            {
               try
               {
                  startSignalOthers.await();
                  for (int j = 0; j < totalTimes; j++)
                  {
                     for (int i = 0; i < totalElement; i++)
                     {
                        map.get(new MyKey("key" + i));
                     }
                     sleep(50);
                  }
               }
               catch (Exception e)
               {
                  errors.add(e);
               }
               finally
               {
                  doneSignal.countDown();
               }
            }
         };
         thread.start();
      }
      for (int i = 0; i < remover; i++)
      {
         Thread thread = new Thread()
         {
            public void run()
            {
               try
               {
                  startSignalOthers.await();
                  for (int j = 0; j < totalTimes; j++)
                  {
                     for (int i = 0; i < totalElement; i++)
                     {
                        map.remove(new MyKey("key" + i));
                     }
                     sleep(50);
                  }
               }
               catch (Exception e)
               {
                  errors.add(e);
               }
               finally
               {
                  doneSignal.countDown();
               }
            }
         };
         thread.start();
      }
      doneSignal.await();
      for (int i = 0; i < totalElement; i++)
      {
         map.put(new MyKey("key" + i), "value" + i);
      }
      assertEquals(totalElement, map.size());
      final CountDownLatch startSignal = new CountDownLatch(1);
      final CountDownLatch doneSignal2 = new CountDownLatch(writer + cleaner);
      for (int i = 0; i < writer; i++)
      {
         Thread thread = new Thread()
         {
            public void run()
            {
               try
               {
                  startSignal.await();
                  for (int j = 0; j < totalTimes; j++)
                  {
                     for (int i = 0; i < totalElement; i++)
                     {
                        map.put(new MyKey("key" + i), "value" + i);
                     }
                     sleep(50);
                  }
               }
               catch (Exception e)
               {
                  errors.add(e);
               }
               finally
               {
                  doneSignal2.countDown();
               }
            }
         };
         thread.start();
      }
      for (int i = 0; i < cleaner; i++)
      {
         Thread thread = new Thread()
         {
            public void run()
            {
               try
               {
                  startSignal.await();
                  for (int j = 0; j < totalTimes; j++)
                  {
                     sleep(150);
                     map.clear();
                  }
               }
               catch (Exception e)
               {
                  errors.add(e);
               }
               finally
               {
                  doneSignal2.countDown();
               }
            }
         };
         thread.start();
      }
      map.clear();
      assertEquals(0, map.size());
      if (!errors.isEmpty())
      {
         for (Exception e : errors)
         {
            e.printStackTrace();
         }
         throw errors.get(0);
      }
      map.clear();
   }

   public static class MyKey implements Serializable
   {
      private static final long serialVersionUID = 1L;

      public String value;

      public MyKey(String value)
      {
         this.value = value;
      }

      @Override
      public boolean equals(Object paramObject)
      {
         return paramObject instanceof MyKey && ((MyKey)paramObject).value.equals(value);
      }

      @Override
      public int hashCode()
      {
         return value.hashCode();
      }

      @Override
      public String toString()
      {
         return value;
      }
   }

   @Test
   public void testBigLoad()
   {
      int times = 300000;
      for (int i = 0; i < times; i++)
      {
         map.put(new MyKey("key" + i), "value" + i);
      }
      callGCNSleep();
      assertTrue(map.isEmpty());
   }

   private static void callGCNSleep()
   {
      System.gc();
      try
      {
         Thread.sleep(20);
      }
      catch (InterruptedException e)
      {
         Thread.currentThread().interrupt();
      }
   }
}
