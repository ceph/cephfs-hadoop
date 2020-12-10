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
 * Abstract base class for communicating with a Ceph filesystem and its
 * C++ codebase from Java, or pretending to do so (for unit testing purposes).
 * As only the Ceph package should be using this directly, all methods
 * are protected.
 */
package org.apache.hadoop.fs.cephrgw;

import java.io.IOException;
import java.net.URI;
import java.net.InetAddress;
import java.util.LinkedList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import com.ceph.rgw.CephStat;
import com.ceph.rgw.CephStatVFS;

abstract class CephFsProto {
    abstract void initialize(URI uri, Configuration conf) throws IOException;

    abstract long __open(Path path, int flags, int mode) throws IOException;

    abstract long open(Path path, int flags, int mode) throws IOException;

    abstract void lstat(Path path, CephStat stat) throws IOException;

    abstract void statfs(Path path, CephStatVFS stat) throws IOException;

    abstract void unlink(Path path) throws IOException;

    abstract int listdir(Path path, LinkedList<String> names, LinkedList<CephStat> stats) throws IOException;

    abstract void setattr(Path path, CephStat stat, int mask) throws IOException;

    abstract void close(long fd) throws IOException;

    abstract void shutdown() throws IOException;

    abstract boolean rename(Path src, Path dst) throws IOException;

    abstract short getDefaultReplication();

    abstract int write(long fd, long offset, byte[] buf, long size) throws IOException;

    abstract int read(long fd, long offset, byte[]buf, long size) throws IOException;

    abstract boolean mkdirs(Path path, int mode) throws IOException;

    abstract void fsync(long fd) throws IOException;
}
