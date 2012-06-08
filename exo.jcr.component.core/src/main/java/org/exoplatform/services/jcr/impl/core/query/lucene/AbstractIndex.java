/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.exoplatform.services.jcr.impl.core.query.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.exoplatform.services.jcr.impl.core.query.IndexerIoMode;
import org.exoplatform.services.jcr.impl.core.query.IndexerIoModeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Implements common functionality for a lucene index.
 * <p/>
 * Note on synchronization: This class is not entirely thread-safe. Certain
 * concurrent access is however allowed. Read-only access on this index using
 * {@link #getReadOnlyIndexReader()} is thread-safe. That is, multiple threads
 * my call that method concurrently and use the returned IndexReader at the same
 * time.<br/>
 * Modifying threads must be synchronized externally in a way that only one
 * thread is using the returned IndexReader and IndexWriter instances returned
 * by {@link #getIndexReader()} and {@link #getIndexWriter()} at a time.<br/>
 * Concurrent access by <b>one</b> modifying thread and multiple read-only
 * threads is safe!
 */
abstract class AbstractIndex
{

   /** The logger instance for this class */
   private static final Logger log = LoggerFactory.getLogger("exo.jcr.component.core.AbstractIndex");

   /** PrintStream that pipes all calls to println(String) into log.info() */
   private static final LoggingPrintStream STREAM_LOGGER = new LoggingPrintStream();

   /** The currently set IndexWriter or <code>null</code> if none is set */
   private IndexWriter indexWriter;

   /** The underlying Directory where the index is stored */
   private Directory directory;

   /** Analyzer we use to tokenize text */
   private Analyzer analyzer;

   /** The similarity in use for indexing and searching. */
   private final Similarity similarity;

   /** Compound file flag */
   private boolean useCompoundFile = true;

   /** maxFieldLength config parameter */
   private int maxFieldLength = SearchIndex.DEFAULT_MAX_FIELD_LENGTH;

   /** termInfosIndexDivisor config parameter */
   private int termInfosIndexDivisor = SearchIndex.DEFAULT_TERM_INFOS_INDEX_DIVISOR;

   /**
    * The document number cache if this index may use one.
    */
   private DocNumberCache cache;

   /** The shared IndexReader for all read-only IndexReaders */
   private SharedIndexReader sharedReader;

   /**
    * The most recent read-only reader if there is any.
    */
   private ReadOnlyIndexReader readOnlyReader;

   /**
    * Flag that indicates whether there was an index present in the directory
    * when this AbstractIndex was created.
    */
   private boolean isExisting;

   protected final IndexerIoModeHandler modeHandler;

   /**
    * Constructs an index with an <code>analyzer</code> and a
    * <code>directory</code>.
    *
    * @param analyzer      the analyzer for text tokenizing.
    * @param similarity    the similarity implementation.
    * @param directory     the underlying directory.
    * @param cache         the document number cache if this index should use
    *                      one; otherwise <code>cache</code> is
    *                      <code>null</code>.
    * @throws IOException if the index cannot be initialized.
    */
   AbstractIndex(final Analyzer analyzer, Similarity similarity, final Directory directory, DocNumberCache cache,
      IndexerIoModeHandler modeHandler) throws IOException
   {
      this.analyzer = analyzer;
      this.similarity = similarity;
      this.directory = directory;
      this.cache = cache;
      this.modeHandler = modeHandler;

      AbstractIndex.this.isExisting = IndexReader.indexExists(directory);

      if (!isExisting)
      {
         IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_30, analyzer);
         indexWriter = new IndexWriter(directory, config);
         // immediately close, now that index has been created
         indexWriter.close();
         indexWriter = null;
      }
   }

   /**
    * Default implementation returns the same instance as passed
    * in the constructor.
    *
    * @return the directory instance passed in the constructor
    */
   Directory getDirectory()
   {
      return directory;
   }

   /**
    * Returns <code>true</code> if this index was openend on a directory with
    * an existing index in it; <code>false</code> otherwise.
    *
    * @return <code>true</code> if there was an index present when this index
    *          was created; <code>false</code> otherwise.
    */
   boolean isExisting()
   {
      return isExisting;
   }

   /**
    * Adds documents to this index and invalidates the shared reader.
    *
    * @param docs the documents to add.
    * @throws IOException if an error occurs while writing to the index.
    */
   void addDocuments(final Document[] docs) throws IOException
   {
      final IndexWriter writer = getIndexWriter();

      IOException ioExc = null;
      try
      {
         for (Document doc : docs)
         {
            try
            {
               writer.addDocument(doc);
            }
            catch (Throwable e)
            {
               if (ioExc == null)
               {
                  if (e instanceof IOException)
                  {
                     ioExc = (IOException)e;
                  }
                  else
                  {
                     ioExc = Util.createIOException(e);
                  }
               }

               log.warn("Exception while inverting document", e);
            }
         }
      }
      finally
      {
         invalidateSharedReader();
      }

      if (ioExc != null)
      {
         throw ioExc;
      }
   }

   /**
    * Defines if index in Read-Write mode according to
    * I/O mode handler or in case of some indexes
    * (for example {@link VolatileIndex}, as it is always in R/W mode)
    * can be overridden to provide yet another logic.
    * 
    */
   protected boolean isReadWriteMode()
   {
      return (modeHandler != null) && modeHandler.getMode() == IndexerIoMode.READ_WRITE;
   }

   /**
    * Removes the document from this index. This call will not invalidate
    * the shared reader. If a subclass whishes to do so, it should overwrite
    * this method and call {@link #invalidateSharedReader()}.
    *
    * @param idTerm the id term of the document to remove.
    * @throws IOException if an error occurs while removing the document.
    * @return number of documents deleted
    */
   int removeDocument(final Term idTerm) throws IOException
   {
      int n = 0;
      ReadOnlyIndexReader readOnlyIndexReader = null;
      TermDocs termDocs = null;

      try
      {
         readOnlyIndexReader = getReadOnlyIndexReader();
         termDocs = readOnlyIndexReader.termDocs(idTerm);

         if (termDocs == null)
         {
            return 0;
         }

         while (termDocs.next())
         {
            readOnlyIndexReader.deleteDocumentTransiently(termDocs.doc());
            n++;
         }

         if (isReadWriteMode())
         {
            getIndexWriter().deleteDocuments(idTerm);
         }
      }
      finally
      {
         if (termDocs != null)
         {
            termDocs.close();
         }

         if (readOnlyIndexReader != null)
         {
            readOnlyIndexReader.decRef();
         }
      }

      return n;
   }


   /**
    * Returns a read-only index reader, that can be used concurrently with
    * other threads writing to this index. The returned index reader is
    * read-only, that is, any attempt to delete a document from the index
    * will throw an <code>UnsupportedOperationException</code>.
    *
    * @param initCache if the caches in the index reader should be initialized
    *          before the index reader is returned.
    * @return a read-only index reader.
    * @throws IOException if an error occurs while obtaining the index reader.
    */
   synchronized ReadOnlyIndexReader getReadOnlyIndexReader(final boolean initCache) throws IOException
   {
      if (readOnlyReader != null)
      {
         readOnlyReader.incRef();
         return readOnlyReader;
      }

      // create new shared reader
      invalidateSharedReader();
      IndexReader reader;
      if (isReadWriteMode())
      {
         reader = IndexReader.open(getIndexWriter(), true);
      }
      else
      {
         reader = IndexReader.open(getDirectory(), termInfosIndexDivisor);
      }

      CachingIndexReader cr = new CachingIndexReader(reader, cache, initCache);
      sharedReader = new SharedIndexReader(cr);

      readOnlyReader = new ReadOnlyIndexReader(sharedReader);
      readOnlyReader.incRef();
      return readOnlyReader;
   }

   /**
    * Returns a read-only index reader, that can be used concurrently with
    * other threads writing to this index. The returned index reader is
    * read-only, that is, any attempt to delete a document from the index
    * will throw an <code>UnsupportedOperationException</code>.
    *
    * @return a read-only index reader.
    * @throws IOException if an error occurs while obtaining the index reader.
    */
   protected ReadOnlyIndexReader getReadOnlyIndexReader() throws IOException
   {
      return getReadOnlyIndexReader(false);
   }

   /**
    * Returns an <code>IndexWriter</code> on this index.
    * @return an <code>IndexWriter</code> on this index.
    * @throws IOException if the writer cannot be obtained.
    */
   protected synchronized IndexWriter getIndexWriter() throws IOException
   {
      if (indexWriter == null)
      {
         IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_30, analyzer);
         config.setSimilarity(similarity);
         if (config.getMergePolicy() instanceof LogMergePolicy)
         {
            ((LogMergePolicy)config.getMergePolicy()).setUseCompoundFile(useCompoundFile);
         }
         else
         {
            log.error("Can't set \"UseCompoundFile\". Merge policy is not an instance of LogMergePolicy. ");
         }
         indexWriter = new IndexWriter(directory, config);
         setUseCompoundFile(useCompoundFile);
         indexWriter.setInfoStream(STREAM_LOGGER);
      }
      return indexWriter;
   }

   /**
    * Commits all pending changes to the underlying <code>Directory</code>.
    * @throws IOException if an error occurs while commiting changes.
    */
   protected void commit() throws IOException
   {
      commit(false);
   }

   /**
    * Commits all pending changes to the underlying <code>Directory</code>.
    *
    * @param optimize if <code>true</code> the index is optimized after the
    *                 commit.
    * @throws IOException if an error occurs while commiting changes.
    */
   protected synchronized void commit(final boolean optimize) throws IOException
   {
      if (indexWriter != null)
      {
         log.debug("committing IndexWriter.");
         indexWriter.commit();
      }
      // optimize if requested
      if (optimize)
      {
         IndexWriter writer = getIndexWriter();
         writer.optimize();
         writer.close();
         indexWriter = null;
      }
   }

   /**
    * Closes this index, releasing all held resources.
    */
   synchronized void close()
   {
      releaseWriterAndReaders();
      if (directory != null)
      {
         try
         {
            directory.close();
         }
         catch (IOException e)
         {
            directory = null;
         }
      }
   }

   /**
    * Releases all potentially held index writer and readers.
    */
   protected void releaseWriterAndReaders()
   {
      if (indexWriter != null)
      {
         try
         {
            indexWriter.close();
         }
         catch (IOException e)
         {
            log.warn("Exception closing index writer: " + e.toString());
         }
         indexWriter = null;
      }
      if (readOnlyReader != null)
      {
         try
         {
            readOnlyReader.decRef();
         }
         catch (IOException e)
         {
            log.warn("Exception closing index reader: " + e.toString());
         }
         readOnlyReader = null;
      }
      if (sharedReader != null)
      {
         try
         {
            sharedReader.decRef();
         }
         catch (IOException e)
         {
            log.warn("Exception closing index reader: " + e.toString());
         }
         sharedReader = null;
      }
   }

   /**
    * @return the number of bytes this index occupies in memory.
    */
   synchronized long getRamSizeInBytes()
   {
      if (indexWriter != null)
      {
         return indexWriter.ramSizeInBytes();
      }
      else
      {
         return 0;
      }
   }

   /**
    * Closes the shared reader.
    *
    * @throws IOException if an error occurs while closing the reader.
    */
   protected synchronized void invalidateSharedReader() throws IOException
   {
      // also close the read-only reader
      if (readOnlyReader != null)
      {
         readOnlyReader.decRef();
         readOnlyReader = null;
      }
      // invalidate shared reader
      if (sharedReader != null)
      {
         sharedReader.decRef();
         sharedReader = null;
      }
   }

   //-------------------------< properties >-----------------------------------

   /**
    * The lucene index writer property: useCompountFile
    */
   void setUseCompoundFile(boolean b)
   {
      useCompoundFile = b;
      if (indexWriter != null)
      {
         IndexWriterConfig config = indexWriter.getConfig();
         if (config.getMergePolicy() instanceof LogMergePolicy)
         {
            ((LogMergePolicy)config.getMergePolicy()).setUseCompoundFile(useCompoundFile);
            ((LogMergePolicy)config.getMergePolicy()).setNoCFSRatio(1.0);
         }
         else
         {
            log.error("Can't set \"UseCompoundFile\". Merge policy is not an instance of LogMergePolicy. ");
         }
      }
   }

   /**
    * The lucene index writer property: maxFieldLength
    */
   void setMaxFieldLength(int maxFieldLength)
   {
      this.maxFieldLength = maxFieldLength;
      if (indexWriter != null)
      {
         indexWriter.setMaxFieldLength(this.maxFieldLength);
      }
   }

   /**
    * @return the current value for termInfosIndexDivisor.
    */
   public int getTermInfosIndexDivisor()
   {
      return termInfosIndexDivisor;
   }

   /**
    * Sets a new value for termInfosIndexDivisor.
    *
    * @param termInfosIndexDivisor the new value.
    */
   public void setTermInfosIndexDivisor(int termInfosIndexDivisor)
   {
      this.termInfosIndexDivisor = termInfosIndexDivisor;
   }

   //------------------------------< internal >--------------------------------

   /**
    * Adapter to pipe info messages from lucene into log messages.
    */
   private static final class LoggingPrintStream extends PrintStream
   {

      /** Buffer print calls until a newline is written */
      private StringBuffer buffer = new StringBuffer();

      public LoggingPrintStream()
      {
         super(new OutputStream()
         {
            @Override
            public void write(int b)
            {
               // do nothing
            }
         });
      }

      @Override
      public void print(String s)
      {
         buffer.append(s);
      }

      @Override
      public void println(String s)
      {
         buffer.append(s);
         log.debug(buffer.toString());
         buffer.setLength(0);
      }
   }
}
