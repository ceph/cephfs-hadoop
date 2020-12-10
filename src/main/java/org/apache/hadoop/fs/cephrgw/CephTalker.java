// -*- mode:Java; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*- 

/**
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * <p>
 * <p>
 * Wraps a number of native function calls to communicate with the Ceph
 * filesystem.
 */
package org.apache.hadoop.fs.cephrgw;

import java.io.IOException;
import java.net.URI;
import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.net.InetAddress;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.lang.StringUtils;

import com.ceph.rgw.CephRgwAdapter;
import com.ceph.rgw.CephStat;
import com.ceph.rgw.CephStatVFS;
import com.ceph.rgw.CephFileAlreadyExistsException;

class CephTalker extends CephFsProto {

    private CephRgwAdapter rgwAdapter;
    private short defaultReplication;
    private String bucket;
    private long cephPos = 0;
    private Log LOG;

    public CephTalker(Configuration conf, Log log) {
        rgwAdapter = null;
        LOG = log;
    }

    private boolean isRootDir(Path path) {
        if (null == path || path.isRoot()) {
            return true;
        }
        return false;
    }

    private String pathString(Path path, boolean isRoot) {
        if (isRoot) {
            return bucket;
        }
        return path.toUri().getPath().substring(1);
    }

    void initialize(URI uri, Configuration conf) throws IOException {
        bucket = uri.getAuthority();
        String args = conf.get(CephConfigKeys.CEPH_RGW_ARGS_KEY, CephConfigKeys.CEPH_RGW_ARGS_DEFAULT);
        rgwAdapter = new CephRgwAdapter(args);

        String userId = conf.get(CephConfigKeys.CEPH_RGW_USERID_KEY, CephConfigKeys.CEPH_RGW_USERID_DEFAULT);

        String accessKey = conf.get(CephConfigKeys.CEPH_RGW_ACCESS_KEY_KEY, CephConfigKeys.CEPH_RGW_ACCESS_KEY_DEFAULT);
        String secretKey = conf.get(CephConfigKeys.CEPH_RGW_SECRET_KEY_KEY, CephConfigKeys.CEPH_RGW_SECRET_KEY_DEFAULT);

        defaultReplication = (short) conf.getInt(CephConfigKeys.CEPH_REPLICATION_KEY,
                CephConfigKeys.CEPH_REPLICATION_DEFAULT);

        try {
            rgwAdapter.mount(userId, accessKey, secretKey, uri.getAuthority());
        } catch (NullPointerException ne) {
            throw new IOException(String.format("mount %s with userId %s accessKey %s secretKey %s failed.",
                    uri.getAuthority(), userId, accessKey, secretKey), ne);
        }
    }

    long __open(Path path, int flags, int mode) throws IOException {
        boolean isRoot = isRootDir(path);
        return rgwAdapter.open(pathString(path, isRoot), isRoot, flags, mode);
    }

    long open(Path path, int flags, int mode) throws IOException {
        return __open(path, flags, mode);
    }

    void lstat(Path path, CephStat stat) throws IOException {
        boolean isRoot = isRootDir(path);
        try {
            rgwAdapter.lstat(pathString(path, isRoot), isRoot, stat);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException();
        }
    }

    void statfs(Path path, CephStatVFS stat) throws IOException {
        boolean isRoot = isRootDir(path);
        try {
            rgwAdapter.statfs(pathString(path, isRoot), isRoot, stat);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException();
        }
    }

    void unlink(Path path) throws IOException {
        boolean isRoot = isRootDir(path);
        rgwAdapter.unlink(pathString(path, isRoot), isRoot);
    }

    boolean rename(Path from, Path to) throws IOException {
        Path src = from.getParent();
        Path dst = to.getParent();
        boolean srcIsRoot = isRootDir(src);
        boolean dstIsRoot = isRootDir(dst);
        try {
            int ret = rgwAdapter.rename(pathString(src, srcIsRoot), srcIsRoot, from.getName(),
                    pathString(dst, dstIsRoot), dstIsRoot, to.getName());
            if (ret == 0)
                return true;
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException();
        }
        return false;
    }

    int listdir(Path path, LinkedList<String> nameList, LinkedList<CephStat> statList) throws IOException {
        boolean isRoot = isRootDir(path);
        return rgwAdapter.listdir(pathString(path, isRoot), isRoot, nameList, statList);
    }

    boolean mkdirs(Path path, int mode) throws IOException {
        Path parent = path.getParent();
        boolean isRoot = isRootDir(parent);
        return rgwAdapter.mkdirs(pathString(parent, isRoot), isRoot, path.getName(), mode);
    }

    void close(long fd) throws IOException {
        rgwAdapter.close(fd);
    }

    void shutdown() throws IOException {
        if (null != rgwAdapter)
            rgwAdapter.unmount();
        rgwAdapter = null;
    }

    short getDefaultReplication() {
        return defaultReplication;
    }

    void setattr(Path path, CephStat stat, int mask) throws IOException {
        boolean isRoot = isRootDir(path);
        rgwAdapter.setattr(pathString(path, isRoot), isRoot, stat, mask);
    }

    void fsync(long fd) throws IOException {
        rgwAdapter.fsync(fd, false);
    }

    int write(long fd, long offset, byte[] buf, long size) throws IOException {
        return (int) rgwAdapter.write(fd, offset, buf, size);
    }

    int read(long fd, long offset, byte[] buf, long size) throws IOException {
        return (int) rgwAdapter.read(fd, offset, buf, size);
    }

}
