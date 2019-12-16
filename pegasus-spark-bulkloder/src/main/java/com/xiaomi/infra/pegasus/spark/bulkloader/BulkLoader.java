package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.alibaba.fastjson.JSON;
import com.xiaomi.infra.pegasus.spark.FDSException;
import com.xiaomi.infra.pegasus.spark.FDSService;
import com.xiaomi.infra.pegasus.spark.RocksDBOptions;
import com.xiaomi.infra.pegasus.spark.bulkloader.DataMetaInfo.FileInfo;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.rocksdb.RocksDBException;
import scala.Tuple2;

public class BulkLoader {
  private static final Log LOG = LogFactory.getLog(BulkLoader.class);

  public static final String BULK_LOAD_INFO = "bulk_load_info";
  private static final String BULK_LOAD_METADATA = "bulk_load_metadata";
  private static final String SST_SUFFIX = ".sst";

  private int curSSTFileIndex = 1;
  private Long curSSTFileSize = 0L;
  private Map<String, Long> sstFileList = new HashMap<>();

  // TODO drop table will result in name-id relationship changed
  private BulkLoadInfo bulkLoadInfo;
  private DataMetaInfo dataMetaInfo;

  private String bulkFilePrefix;
  private String partitionPath;
  private String bulkLoadInfoPath;
  private String bulkLoadMetaDataPath;

  private FDSService fdsService;
  private SSTWriter sstWriter;

  private Iterator<Tuple2<RocksDBRecord, String>> dataResourceIterator;

  public BulkLoader(
      BulkLoaderConfig config, Iterator<Tuple2<RocksDBRecord, String>> iterator, int partitionId) {
    this.dataResourceIterator = iterator;

    this.bulkFilePrefix =
        config.remoteFsUrl
            + config.pathRoot
            + "/"
            + config.clusterName
            + "/"
            + config.tableName
            + "/";
    this.partitionPath = bulkFilePrefix + "/" + partitionId + "/";
    this.bulkLoadInfoPath = bulkFilePrefix + "/" + BULK_LOAD_INFO;
    this.bulkLoadMetaDataPath = partitionPath + "/" + BULK_LOAD_METADATA;

    this.bulkLoadInfo =
        new BulkLoadInfo(
            config.clusterName, config.tableName, config.tableId, config.tablePartitionCount);
    this.dataMetaInfo = new DataMetaInfo();

    this.fdsService = new FDSService();
    this.sstWriter =
        new SSTWriter(new RocksDBOptions(config), config.tablePartitionCount, partitionId);
  }

  void write() throws IOException, RocksDBException, URISyntaxException, FDSException {
    // TODO bulkLoadInfoFile will be write multi time in distributed system or multi thread
    createBulkLoadInfoFile();
    createSSTFile(dataResourceIterator);
    createBulkLoadMetaDataFile();
  }

  private void createSSTFile(Iterator<Tuple2<RocksDBRecord, String>> iterator)
      throws RocksDBException {
    String curSSTFileName = curSSTFileIndex + SST_SUFFIX;
    sstWriter.open(partitionPath + curSSTFileName);
    while (iterator.hasNext()) {
      RocksDBRecord rocksDBRecord = iterator.next()._1;
      if (curSSTFileSize > 64 * 1024 * 1024) {
        sstFileList.put(curSSTFileIndex + SST_SUFFIX, curSSTFileSize);
        curSSTFileIndex++;
        curSSTFileSize = 0L;
        curSSTFileName = curSSTFileIndex + SST_SUFFIX;
        sstWriter.close();

        sstWriter.open(partitionPath + curSSTFileName);
      }
      curSSTFileSize += sstWriter.write(rocksDBRecord.key(), rocksDBRecord.value());
    }
    if (curSSTFileSize != 0) {
      sstWriter.close();
    } else {
      // sstWriter will be throw exception when closed if sstWriter don't write any kv,
      sstWriter.writeNoHashCheck();
      sstWriter.close();
    }
  }

  private void createBulkLoadInfoFile() throws IOException, URISyntaxException, FDSException {
    BufferedWriter bulkLoadInfoWriter = fdsService.getWriter(bulkLoadInfoPath);
    bulkLoadInfoWriter.write(JSON.toJSONString(bulkLoadInfo));
    bulkLoadInfoWriter.close();
  }

  private void createBulkLoadMetaDataFile() throws IOException, URISyntaxException, FDSException {
    long totalSize = 0;
    BufferedWriter bulkLoadMetaDataWriter = fdsService.getWriter(bulkLoadMetaDataPath);
    FileStatus[] fileStatuses = fdsService.getFileStatus(partitionPath);
    for (FileStatus fileStatus : fileStatuses) {
      String filePath = fileStatus.getPath().toString();

      String fileName = fileStatus.getPath().getName();
      long fileSize = fileStatus.getLen();
      String fileMD5 = fdsService.getMD5(filePath);

      FileInfo fileInfo = dataMetaInfo.new FileInfo(fileName, fileSize, fileMD5);
      dataMetaInfo.files.add(fileInfo);

      totalSize += fileSize;
    }
    dataMetaInfo.file_total_size = totalSize;
    bulkLoadMetaDataWriter.write(JSON.toJSONString(dataMetaInfo));
    bulkLoadMetaDataWriter.close();
  }
}
