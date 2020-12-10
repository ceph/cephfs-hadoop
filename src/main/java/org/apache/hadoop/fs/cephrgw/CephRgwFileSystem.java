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
 * Implements the Hadoop FS interfaces to allow applications to store
 * files in Ceph.
 */
package org.apache.hadoop.fs.cephrgw;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.InetAddress;
import java.util.EnumSet;
import java.lang.Math;
import java.util.LinkedList;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.fs.FsStatus;

import com.ceph.rgw.CephFileAlreadyExistsException;
import com.ceph.rgw.CephRgwAdapter;
import com.ceph.rgw.CephStat;
import com.ceph.rgw.CephStatVFS;


public class CephRgwFileSystem extends FileSystem {
    private static final Log LOG = LogFactory.getLog(CephRgwFileSystem.class);
    private URI uri;

    private Path workingDir;
    private CephFsProto ceph = null;

    public CephRgwFileSystem() {
    }

    public CephRgwFileSystem(Configuration conf) {
        setConf(conf);
    }

    /**
     * Create an absolute path using the working directory.
     */
    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }

    public URI getUri() {
        return uri;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        if (ceph == null) {
            ceph = new CephTalker(conf, LOG);
        }
        ceph.initialize(uri, conf);
        setConf(conf);
        try {
            this.uri = new URI(uri.getScheme(), uri.getAuthority(), "", "");
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        this.workingDir = getHomeDirectory();
    }

    /**
     * Open a Ceph file and attach the file handle to an FSDataInputStream.
     * @param path The file to open
     * @param bufferSize Ceph does internal buffering; but you can buffer in
     *   the Java code too if you like.
     * @return FSDataInputStream reading from the given path.
     * @throws IOException if the path DNE or is a
     * directory, or there is an error getting data to set up the FSDataInputStream.
     */
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        path = makeAbsolute(path);

        long fd = ceph.open(path, CephRgwAdapter.O_RDONLY, 0);

        CephStat stat = new CephStat();
        ceph.lstat(path, stat);

        CephInputStream istream = new CephInputStream(getConf(), ceph, fd, stat.size, bufferSize);
        return new FSDataInputStream(istream);
    }

    /**
     * Close down the CephFileSystem. Runs the base-class close method
     * and then kills the Ceph client itself.
     */
    @Override
    public void close() throws IOException {
        super.close();
        ceph.shutdown();
    }

    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Append is not supported " + "by CephRgwFileSystem");
    }

    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public void setWorkingDirectory(Path dir) {
        workingDir = makeAbsolute(dir);
    }

    /**
     * Create a directory and any nonexistent parents. Any portion
     * of the directory tree can exist without error.
     * @param path The directory path to create
     * @param perms The permissions to apply to the created directories.
     * @return true if successful, false otherwise
     * @throws IOException if the path is a child of a file.
     */
    @Override
    public boolean mkdirs(Path path, FsPermission perms) throws IOException {
        path = makeAbsolute(path);

        Path parent = path.getParent();
        if (parent == null) {
            return true;
        }

        if (exists(path)) {
            if (getFileStatus(path).isFile()) {
                throw new CephFileAlreadyExistsException(path.toString());
            }
            return true;
        }

        boolean ret = mkdirs(parent, perms);
        if (!ret)
            return ret;
        try {
            ret = ceph.mkdirs(path, (int) perms.toShort());
        } catch (CephFileAlreadyExistsException e) {
            ret = true;
        }

        return ret;
    }

    /**
     * Create a directory and any nonexistent parents. Any portion
     * of the directory tree can exist without error.
     * Apply umask from conf
     * @param f The directory path to create
     * @return true if successful, false otherwise
     * @throws IOException if the path is a child of a file.
     */
    @Override
    public boolean mkdirs(Path f) throws IOException {
        return mkdirs(f, FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(getConf())));
    }

    /**
     * Get stat information on a file. This does not fill owner or group, as
     * Ceph's support for these is a bit different than HDFS'.
     * @param path The path to stat.
     * @return FileStatus object containing the stat information.
     * @throws FileNotFoundException if the path could not be resolved.
     */
    public FileStatus getFileStatus(Path path) throws IOException {
        path = makeAbsolute(path);

        CephStat stat = new CephStat();
        ceph.lstat(path, stat);

        FileStatus status = new FileStatus(stat.size, stat.isDir(),
                ceph.getDefaultReplication(), stat.blksize, stat.m_time,
                stat.a_time, new FsPermission((short) stat.mode),
                System.getProperty("user.name"), null, path.makeQualified(this));

        return status;
    }

    /**
     * Get the FileStatus for each listing in a directory.
     * @param path The directory to get listings from.
     * @return FileStatus[] containing one FileStatus for each directory listing;
     *         null if path does not exist.
     */
    public FileStatus[] listStatus(Path path) throws IOException {
        path = makeAbsolute(path);

        LinkedList<String> names = new LinkedList<String>();
        LinkedList<CephStat> stats = new LinkedList<CephStat>();
        int len = ceph.listdir(path, names, stats);
        if (len < 0)
            throw new FileNotFoundException("File " + path + " does not exist.");
        else if (len == 0)
            return new FileStatus[0];

        FileStatus[] status = new FileStatus[len];
        for (int i = 0; i < len; i++) {
            CephStat stat = stats.get(i);
            status[i] = new FileStatus(stat.size, stat.isDir(),
                    ceph.getDefaultReplication(), stat.blksize, stat.m_time,
                    stat.a_time, new FsPermission((short) stat.mode),
                    System.getProperty("user.name"), null, makeQualified(new Path(path, names.get(i))));
        }
        names.clear();
        stats.clear();
        return status;
    }

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        path = makeAbsolute(path);

        CephStat stat = new CephStat(permission.toShort());
        ceph.setattr(path, stat, CephRgwAdapter.SETATTR_MODE);
    }

    @Override
    public void setTimes(Path path, long mtime, long atime) throws IOException {
        path = makeAbsolute(path);

        CephStat stat = new CephStat(mtime, atime);
        int mask = 0;

        if (mtime != -1) {
            mask |= CephRgwAdapter.SETATTR_MTIME;
        }

        if (atime != -1) {
            mask |= CephRgwAdapter.SETATTR_ATIME;
        }

        ceph.setattr(path, stat, mask);
    }

    /**
     * Create a new file and open an FSDataOutputStream that's connected to it.
     * @param path The file to create.
     * @param permission The permissions to apply to the file.
     * @param overwrite If true, overwrite any existing file with
           * this name; otherwise don't.
     * @param bufferSize Ceph does internal buffering, but you can buffer
     *   in the Java code too if you like.
     * @param replication Replication factor. See documentation on the
     *   "ceph.data.pools" configuration option.
     * @param blockSize Ignored by Ceph. You can set client-wide block sizes
     * via the fs.ceph.blockSize param if you like.
     * @param progress A Progressable to report back to.
     * Reporting is limited but exists.
     * @return An FSDataOutputStream pointing to the created file.
     * @throws IOException if the path is an
     * existing directory, or the path exists but overwrite is false, or there is a
     * failure in attempting to open for append with Ceph.
     */
    public FSDataOutputStream create(Path path, FsPermission permission,
                                     boolean overwrite, int bufferSize, short replication, long blockSize,
                                     Progressable progress) throws IOException {
        path = makeAbsolute(path);

        boolean exists = exists(path);

        if (progress != null) {
            progress.progress();
        }

        int flags = CephRgwAdapter.O_WRONLY | CephRgwAdapter.O_CREAT;

        if (exists) {
            if (overwrite)
                flags |= CephRgwAdapter.O_TRUNC;
            else
                throw new FileAlreadyExistsException();
        } else {
            Path parent = path.getParent();
            if (parent != null)
                if (!mkdirs(parent))
                    throw new IOException("mkdirs failed for " + parent.toString());
        }

        if (progress != null) {
            progress.progress();
        }

        if (blockSize > Integer.MAX_VALUE) {
            blockSize = Integer.MAX_VALUE;
            LOG.info("blockSize too large. Rounding down to " + blockSize);
        }

        if (blockSize <= 0)
            throw new IllegalArgumentException("Invalid block size: " + blockSize);

        long fd = ceph.open(path, flags, (int) permission.toShort());

        if (progress != null) {
            progress.progress();
        }

        OutputStream ostream = new CephOutputStream(getConf(), ceph, fd, bufferSize);
        return new FSDataOutputStream(ostream, statistics);
    }

    /**
     * Opens an FSDataOutputStream at the indicated Path with write-progress
     * reporting. Same as create(), except fails if parent directory doesn't
     * already exist.
     * @param path the file name to open
     * @param permission The permissions to apply to the file.
     * @param overwrite if a file with this name already exists, then if true,
     * the file will be overwritten, and if false an error will be thrown.
     * @param bufferSize the size of the buffer to be used.
     * @param replication required block replication for the file.
     * @param blockSize Ignored by Ceph. You can set client-wide block sizes
     * via the fs.ceph.blockSize param if you like.
     * @param progress A Progressable to report back to.
     * Reporting is limited but exists.
     * @throws IOException if the path is an
     * existing directory, or the path exists but overwrite is false, or there is a
     * failure in attempting to open for append with Ceph.
     * @deprecated API only for 0.20-append
     */
     @Deprecated
    public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
                                                 boolean overwrite,
                                                 int bufferSize, short replication, long blockSize,
                                                 Progressable progress) throws IOException {

        path = makeAbsolute(path);

        Path parent = path.getParent();

        if (parent != null) {
            CephStat stat = new CephStat();
            ceph.lstat(parent, stat); // handles FileNotFoundException case
            if (stat.isFile())
                throw new FileAlreadyExistsException(parent.toString());
        }

        return this.create(path, permission, overwrite,
                bufferSize, replication, blockSize, progress);
    }

    /**
     * Rename a file or directory.
     * @param src The current path of the file/directory
     * @param dst The new name for the path.
     * @return true if the rename succeeded, false otherwise.
     */
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        boolean ret = false;
        src = makeAbsolute(src);
        dst = makeAbsolute(dst);

        try {
            ret = ceph.rename(src, dst);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (Exception e) {
            return ret;
        }

        return ret;
    }

    @Deprecated
    public boolean delete(Path path) throws IOException {
        return delete(path, false);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        path = makeAbsolute(path);

        /* path exists? */
        FileStatus status;
        try {
            status = getFileStatus(path);
        } catch (FileNotFoundException e) {
            return false;
        }

        /* we're done if its a file */
        if (status.isFile()) {
            ceph.unlink(path);
            return true;
        }

        /* get directory contents */
        FileStatus[] dirlist = listStatus(path);
        if (dirlist == null)
            return false;

        if (!recursive && dirlist.length > 0)
            throw new IOException("Directory " + path.toString() + "is not empty.");

        for (FileStatus fs : dirlist) {
            if (!delete(fs.getPath(), recursive))
                return false;
        }

        ceph.unlink(path);
        return true;
    }

    @Override
    public short getDefaultReplication() {
        return ceph.getDefaultReplication();
    }

    @Override
    public long getDefaultBlockSize() {
        return getConf().getLong(
                CephConfigKeys.CEPH_RGW_BLOCKSIZE_KEY,
                CephConfigKeys.CEPH_RGW_BLOCKSIZE_DEFAULT);
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        CephStatVFS stat = new CephStatVFS();
        ceph.statfs(p, stat);

        FsStatus status = new FsStatus(stat.bsize * stat.blocks,
                stat.bsize * (stat.blocks - stat.bavail),
                stat.bsize * stat.bavail);
        return status;
    }
}


