/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.bytes;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncMultiBufferInputStream extends MultiBufferInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncMultiBufferInputStream.class);

  final SeekableInputStream fileInputStream;
  int fetchIndex = 0;
  int readIndex = 0;
  ExecutorService threadPool;
  LinkedBlockingQueue<Future<Void>> readFutures;
  boolean closed = false;

  AsyncMultiBufferInputStream(ExecutorService threadPool, SeekableInputStream fileInputStream,
    List<ByteBuffer> buffers) {
    super(buffers);
    this.fileInputStream = fileInputStream;
    this.threadPool = threadPool;
    readFutures = new LinkedBlockingQueue<>(buffers.size());
    if (LOG.isDebugEnabled()) {
      LOG.debug("ASYNC: Begin read into buffers ");
      for (ByteBuffer buf : buffers) {
        LOG.debug("ASYNC: buffer {} ", buf);
      }
    }

    fetchAll();
    nextBuffer();
  }

  private void checkNotClosed() {
    if (closed) {
      throw new RuntimeException("Stream is closed");
    }
  }

  private void fetchAll() {
    checkNotClosed();
    try {
      readFutures.put(
        threadPool.submit(new ReadPageDataTask(this)));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  protected boolean nextBuffer() {
    checkNotClosed();
    // hack: parent constructor can call this method before this class is fully initialized.
    // Just return without doing anything.
    if (readFutures == null) {
      return false;
    }
    if (readIndex < buffers.size()) {
      long start = System.nanoTime();
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("ASYNC (next): Getting next buffer");
        }
        Future<Void> readResult;
        readResult = readFutures.take();
        readResult.get();
        long timeSpent = System.nanoTime() - start;
        if (LOG.isDebugEnabled()) {
          LOG.debug("ASYNC (next): {}: Time blocked for read {} ns", this, timeSpent);
        }
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        LOG.error("Async (next): exception while getting next buffer: ", e);
        throw new RuntimeException(e);
      }
      readIndex++;
    }
    return super.nextBuffer();
  }

  public void close() {
    Future<Void> readResult;
    while(!readFutures.isEmpty()) {
      try {
        readResult = readFutures.poll();
        readResult.get();
        if(!readResult.isDone() && !readResult.isCancelled()){
          readResult.cancel(true);
        } else {
          readResult.get(1, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        // Do nothing
      }
    }
    closed = true;
  }


  public static class ReadPageDataTask implements Callable<Void> {

    final AsyncMultiBufferInputStream parent;

    ReadPageDataTask(AsyncMultiBufferInputStream parent) {
      this.parent = parent;
    }

    public Void call() {
      boolean eof = false;
      try {
        ByteBuffer buffer;
        buffer = parent.buffers.get(parent.fetchIndex);
        if (LOG.isDebugEnabled()) {
          LOG.debug("ASYNC: Begin disk read ({} bytes) into buffer at index {}  and buffer {}",
            buffer.remaining(), parent.fetchIndex, buffer);
        }
        long start = System.nanoTime();
        try {
          parent.fileInputStream.readFully(buffer);
        } catch (EOFException e) {
          LOG.debug("Hit EOF reading into {} from  {}", buffer, parent.fileInputStream);
          eof = true;
        }
        buffer.flip();
        long timeSpent = System.nanoTime() - start;
        if (LOG.isDebugEnabled()) {
          LOG.debug("ASYNC: {}: Time to read {} bytes from disk :  {} ns", this, buffer.remaining(),
            timeSpent);
        }
        parent.fetchIndex++;
        if (!eof && parent.fetchIndex < parent.buffers.size()) {
          try {
            parent.readFutures.put(
              parent.threadPool.submit(new ReadPageDataTask(parent)));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    }
  } //PageDataReaderTask

}
