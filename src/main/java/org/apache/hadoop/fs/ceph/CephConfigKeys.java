/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ceph;

import org.apache.hadoop.fs.CommonConfigurationKeys;

/**
 * Configuration key constants used by CephFileSystem.
 */
public class CephConfigKeys extends CommonConfigurationKeys {
  public static final String CEPH_OBJECT_SIZE_KEY = "ceph.object.size";
  public static final long   CEPH_OBJECT_SIZE_DEFAULT = 64*1024*1024;

  public static final String CEPH_CONF_FILE_KEY = "ceph.conf.file";
  public static final String CEPH_CONF_FILE_DEFAULT = null;

  public static final String CEPH_CONF_OPTS_KEY = "ceph.conf.options";
  public static final String CEPH_CONF_OPTS_DEFAULT = null;

  public static final String CEPH_REPLICATION_KEY = "ceph.replication";
  public static final short  CEPH_REPLICATION_DEFAULT = 3;

  public static final String CEPH_ROOT_DIR_KEY = "ceph.root.dir";
  public static final String CEPH_ROOT_DIR_DEFAULT = null;

  public static final String  CEPH_LOCALIZE_READS_KEY = "ceph.localize.reads";
  public static final boolean CEPH_LOCALIZE_READS_DEFAULT = true;

  public static final String CEPH_DATA_POOLS_KEY = "ceph.data.pools";
  public static final String CEPH_DATA_POOLS_DEFAULT = null;

  public static final String CEPH_AUTH_ID_KEY = "ceph.auth.id";
  public static final String CEPH_AUTH_ID_DEFAULT = null;

  public static final String CEPH_AUTH_KEYFILE_KEY = "ceph.auth.keyfile";
  public static final String CEPH_AUTH_KEYFILE_DEFAULT = null;

  public static final String CEPH_AUTH_KEYRING_KEY = "ceph.auth.keyring";
  public static final String CEPH_AUTH_KEYRING_DEFAULT = null;

  public static final String CEPH_MON_ADDR_KEY = "ceph.mon.address";
  public static final String CEPH_MON_ADDR_DEFAULT = null;
}
