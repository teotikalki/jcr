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

import org.jboss.util.collection.ValueRef;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This map will remove entries when the value in the map has been
 * cleaned from garbage collection.
 * 
 * This map is thread safe and provides the additional methods required to
 * be able to use it as {@link ConcurrentMap}
 * 
 * @author <a href="mailto:nfilotto@exoplatform.com">Nicolas Filotto</a>
 * @version $Id$
 *
 */
public class ConcurrentWeakValueMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V>
{

   /** 
    * Hash table mapping keys to ref values
    */
   private final ConcurrentMap<K, ValueRef<K, V>> map;

   /** 
    * Reference queue for cleared RefKeys
    */
   private final ReferenceQueue<V> queue = new ReferenceQueue<V>();

   /**
    * Creates a new, empty map with the specified initial
    * capacity, load factor and concurrency level.
    *
    * @param initialCapacity the initial capacity. The implementation
    * performs internal sizing to accommodate this many elements.
    * @param loadFactor  the load factor threshold, used to control resizing.
    * Resizing may be performed when the average number of elements per
    * bin exceeds this threshold.
    * @param concurrencyLevel the estimated number of concurrently
    * updating threads. The implementation performs internal sizing
    * to try to accommodate this many threads.
    * @throws IllegalArgumentException if the initial capacity is
    * negative or the load factor or concurrencyLevel are
    * nonpositive.
    */
   public ConcurrentWeakValueMap(int initialCapacity, float loadFactor, int concurrencyLevel)
   {
      this.map = new ConcurrentHashMap<K, ValueRef<K, V>>(initialCapacity, loadFactor, concurrencyLevel);
   }

   /**
    * Creates a new, empty map with the specified initial capacity
    * and load factor and with the default concurrencyLevel (16).
    *
    * @param initialCapacity The implementation performs internal
    * sizing to accommodate this many elements.
    * @param loadFactor  the load factor threshold, used to control resizing.
    * Resizing may be performed when the average number of elements per
    * bin exceeds this threshold.
    * @throws IllegalArgumentException if the initial capacity of
    * elements is negative or the load factor is nonpositive
    */
   public ConcurrentWeakValueMap(int initialCapacity, float loadFactor)
   {
      this.map = new ConcurrentHashMap<K, ValueRef<K, V>>(initialCapacity, loadFactor);
   }

   /**
    * Creates a new, empty map with the specified initial capacity,
    * and with default load factor (0.75) and concurrencyLevel (16).
    *
    * @param initialCapacity the initial capacity. The implementation
    * performs internal sizing to accommodate this many elements.
    * @throws IllegalArgumentException if the initial capacity of
    * elements is negative.
    */
   public ConcurrentWeakValueMap(int initialCapacity)
   {
      this.map = new ConcurrentHashMap<K, ValueRef<K, V>>(initialCapacity);
   }

   /**
    * Creates a new, empty map with a default initial capacity (16),
    * load factor (0.75) and concurrencyLevel (16).
    */
   public ConcurrentWeakValueMap()
   {
      this.map = new ConcurrentHashMap<K, ValueRef<K, V>>();
   }

   /**
    * {@inheritDoc}
    */
   public V putIfAbsent(K key, V value)
   {
      processQueue();
      ValueRef<K, V> ref = create(key, value, queue);
      ValueRef<K, V> result;
      while ((result = map.putIfAbsent(key, ref)) != null)
      {
         V v = result.get();
         if (v != null)
         {
            return v;
         }
         map.remove(key, result);
      }
      return null;
   }

   /**
    * {@inheritDoc}
    */
   public boolean remove(Object key, Object value)
   {
      processQueue();
      ValueRef<K, V> ref = map.get(key);
      if (ref != null)
      {
         V v = ref.get();
         if (value == null || v == null || value.equals(v))
         {
            if (map.remove(key, ref) && v != null)
               return true;
         }
      }
      return false;
   }

   /**
    * {@inheritDoc}
    */
   public boolean replace(K key, V oldValue, V newValue)
   {
      processQueue();
      ValueRef<K, V> ref = map.get(key);
      if (ref != null)
      {
         V v = ref.get();
         if ((oldValue == null && v == null) || oldValue.equals(v))
         {
            ValueRef<K, V> newRef = create(key, newValue, queue);
            return map.replace(key, ref, newRef);
         }
      }
      return false;
   }

   /**
    * {@inheritDoc}
    */
   public V replace(K key, V value)
   {
      processQueue();
      ValueRef<K, V> ref = create(key, value, queue);
      ValueRef<K, V> result = map.replace(key, ref);
      if (result != null)
      {
         V v = result.get();
         if (v == null)
            map.remove(key, ref);
         return v;
      }
      return null;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int size()
   {
      processQueue();
      return map.size();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean containsKey(Object key)
   {
      processQueue();
      return map.containsKey(key);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public V get(Object key)
   {
      processQueue();
      ValueRef<K, V> ref = map.get(key);
      if (ref != null)
      {
         V v = ref.get();
         if (v == null)
            map.remove(key, ref);
         return v;
      }
      return null;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public V put(K key, V value)
   {
      processQueue();
      ValueRef<K, V> ref = create(key, value, queue);
      ValueRef<K, V> result = map.put(key, ref);
      if (result != null)
         return result.get();
      return null;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public V remove(Object key)
   {
      processQueue();
      ValueRef<K, V> result = map.remove(key);
      if (result != null)
         return result.get();
      return null;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Set<Entry<K,V>> entrySet()
   {
      processQueue();
      return new EntrySet();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void clear()
   {
      processQueue();
      map.clear();
   }

   /**
    * Remove all entries whose values have been discarded.
    */
   @SuppressWarnings("unchecked")
   private void processQueue()
   {
      ValueRef<K, V> ref = (ValueRef<K, V>) queue.poll();
      while (ref != null)
      {
         // only remove if it is the *exact* same WeakValueRef
         map.remove(ref.getKey(), ref);

         ref = (ValueRef<K, V>) queue.poll();
      }
   }

   /**
    * EntrySet.
    */
   private class EntrySet extends AbstractSet<Entry<K, V>>
   {
      @Override
      public Iterator<Entry<K, V>> iterator()
      {
         return new EntrySetIterator(map.entrySet().iterator());
      }

      @Override
      public int size()
      {
         return ConcurrentWeakValueMap.this.size();
      }
   }

   /**
    * EntrySet iterator
    */
   private class EntrySetIterator implements Iterator<Entry<K, V>>
   {
      /** 
       * The delegate
       */
      private final Iterator<Entry<K, ValueRef<K, V>>> delegate;

      /**
       * Create a new EntrySetIterator.
       *
       * @param delegate the delegate
       */
      public EntrySetIterator(Iterator<Entry<K, ValueRef<K, V>>> delegate)
      {
         this.delegate = delegate;
      }

      public boolean hasNext()
      {
         return delegate.hasNext();
      }

      public Entry<K, V> next()
      {
         Entry<K, ValueRef<K, V>> next = delegate.next();
         return next.getValue();
      }

      /**
       * {@inheritDoc}
       */
      public void remove()
      {
         delegate.remove();
      }
   }

   /**
    * Create new value ref instance.
    *
    * @param key the key
    * @param value the value
    * @param q the ref queue
    * @return new value ref instance
    */
   protected ValueRef<K, V> create(K key, V value, ReferenceQueue<V> q)
   {
      return WeakValueRef.create(key, value, q);
   }

   @Override
   public String toString()
   {
      return map.toString();
   }

   private static class WeakValueRef<K, V> extends WeakReference<V> implements ValueRef<K, V>
   {
      /**
       * The key
       */
      private final K key;

      /**
       * Safely create a new WeakValueRef
       *
       * @param <K> the key type
       * @param <V> the value type
       * @param key the key
       * @param val the value
       * @param q   the reference queue
       * @return the reference or null if the value is null
       */
      static <K, V> WeakValueRef<K, V> create(K key, V val, ReferenceQueue<V> q)
      {
         if (val == null)
            return null;
         else
            return new WeakValueRef<K, V>(key, val, q);
      }

      /**
       * Create a new WeakValueRef.
       *
       * @param key the key
       * @param val the value
       * @param q   the reference queue
       */
      private WeakValueRef(K key, V val, ReferenceQueue<V> q)
      {
         super(val, q);
         this.key = key;
      }

      public K getKey()
      {
         return key;
      }

      public V getValue()
      {
         return get();
      }

      public V setValue(V value)
      {
         throw new UnsupportedOperationException("setValue");
      }

      @Override
      public String toString()
      {
         return String.valueOf(get());
      }
   }
}
