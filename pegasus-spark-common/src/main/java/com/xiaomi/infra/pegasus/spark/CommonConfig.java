package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

/** The config class contains the common config for coldBackupConfig and bulkLoadConfig */
public abstract class CommonConfig implements Serializable {

  private String remoteFileSystemURL;
  private String remoteFileSystemPort;
  private String clusterName;
  private String tableName;

  protected CommonConfig(String url, String port, String clusterName, String tableName) {
    this.remoteFileSystemURL = url;
    this.remoteFileSystemPort = port;
    this.clusterName = clusterName;
    this.tableName = tableName;
  }

  public void setRemoteFileSystemURL(String remoteFileSystemURL) {
    this.remoteFileSystemURL = remoteFileSystemURL;
  }

  public void setRemoteFileSystemPort(String remoteFileSystemPort) {
    this.remoteFileSystemPort = remoteFileSystemPort;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getRemoteFileSystemURL() {
    return remoteFileSystemURL;
  }

  public String getRemoteFileSystemPort() {
    return remoteFileSystemPort;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getTableName() {
    return tableName;
  }
}
