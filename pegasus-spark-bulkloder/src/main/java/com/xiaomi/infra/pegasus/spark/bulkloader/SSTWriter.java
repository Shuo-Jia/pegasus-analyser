package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.RocksDBOptions;
import com.xiaomi.infra.pegasus.spark.Tools;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

public class SSTWriter implements AutoCloseable {

  SstFileWriter sstFileWriter;
  int partitionCount;
  int partitionId;
  RocksDBOptions rocksDBOptions;

  public SSTWriter(RocksDBOptions rocksDBOptions, int partitionCount, int partitionId) {
    this.rocksDBOptions = rocksDBOptions;
    this.partitionCount = partitionCount;
    this.partitionId = partitionId;
    this.sstFileWriter = new SstFileWriter(rocksDBOptions.envOptions, rocksDBOptions.options);
  }

  public int write(byte[] key, byte[] value) throws RocksDBException {
    if (isCurrentPid(key)) {
      try {
        sstFileWriter.put(key, value);
      } catch (RocksDBException e) {
        throw new RocksDBException(e.getMessage());
      }
      return key.length + value.length;
    }
    return 0;
  }

  /**
   * only for writing "null-null" kv, see {@link BulkLoader} createSSTFile method
   *
   * @throws RocksDBException
   */
  void writeNoHashCheck() throws RocksDBException {
    byte[] pegasusKey = RocksDBRecord.generateKey("null".getBytes(), "null".getBytes());
    byte[] pegasusValue = RocksDBRecord.generateValue("null".getBytes());
    sstFileWriter.put(pegasusKey, pegasusValue);
  }

  void open(String path) throws RocksDBException {
    sstFileWriter.open(path);
  }

  @Override
  public void close() throws RocksDBException {
    sstFileWriter.finish();
  }

  private boolean isCurrentPid(byte[] pegasusKey) {
    return Tools.remainderUnsigned(Tools.hash(pegasusKey), partitionCount) == partitionId;
  }
}
