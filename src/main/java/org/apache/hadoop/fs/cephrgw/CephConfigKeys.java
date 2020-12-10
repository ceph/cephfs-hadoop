/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cephrgw;

import org.apache.hadoop.fs.CommonConfigurationKeys;


/**
 * Configuration key constants used by CephRgwFileSystem.
 */
public class CephConfigKeys extends CommonConfigurationKeys {
    public static final String CEPH_RGW_ARGS_KEY = "fs.ceph.rgw.args";
    public static final String CEPH_RGW_ARGS_DEFAULT = "--name=client.admin";

    public static final String CEPH_RGW_BLOCKSIZE_KEY = "fs.ceph.rgw.blocksize";
    public static final long CEPH_RGW_BLOCKSIZE_DEFAULT = 64 * 1024 * 1024;

    public static final String CEPH_REPLICATION_KEY = "fs.ceph.replication";
    public static final short CEPH_REPLICATION_DEFAULT = 3;

    public static final String CEPH_RGW_USERID_KEY = "fs.ceph.rgw.userid";
    public static final String CEPH_RGW_USERID_DEFAULT = null;

    public static final String CEPH_RGW_ACCESS_KEY_KEY = "fs.ceph.rgw.access.key";
    public static final String CEPH_RGW_ACCESS_KEY_DEFAULT = null;

    public static final String CEPH_RGW_SECRET_KEY_KEY = "fs.ceph.rgw.secret.key";
    public static final String CEPH_RGW_SECRET_KEY_DEFAULT = null;
}
