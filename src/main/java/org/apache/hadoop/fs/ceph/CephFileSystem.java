// -*- mode:Java; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*-

/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 *
 * Implements the Hadoop FS interfaces to allow applications to store
 * files in Ceph.
 */
package org.apache.hadoop.fs.ceph;


import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.URI;
import java.net.InetAddress;
import java.util.EnumSet;
import java.lang.Math;
import java.util.ArrayList;
import java.util.Map;
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

import com.ceph.fs.CephFileAlreadyExistsException;
import com.ceph.fs.CephNotDirectoryException;
import com.ceph.fs.CephMount;
import com.ceph.fs.CephStat;
import com.ceph.crush.Bucket;
import com.ceph.fs.CephFileExtent;


/**
 * Known Issues:
 *
 *   1. Per-file replication and block size are ignored.
 */
public class CephFileSystem extends FileSystem {
  private static final Log LOG = LogFactory.getLog(CephFileSystem.class);
  private URI uri;

  private Path workingDir;
  private CephFS ceph;
  private static final int CEPH_STRIPE_COUNT = 1;
  private TreeMap<Integer, String> datapools = null;

  /**
   * Create a new CephFileSystem.
   */
  public CephFileSystem() {
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

  /** {@inheritDoc} */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    if (ceph == null) {
      ceph = new CephTalker(conf, LOG);
    }
    ceph.initialize(uri, conf);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
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

    int fd = ceph.open(path, CephMount.O_RDONLY, 0);

    /* get file size */
    CephStat stat = new CephStat();
    ceph.fstat(fd, stat);

    CephInputStream istream = new CephInputStream(getConf(), ceph, fd,
        stat.size, bufferSize);
    return new FSDataInputStream(istream);
  }

  /**
   * Close down the CephFileSystem. Runs the base-class close method
   * and then kills the Ceph client itself.
   */
  @Override
  public void close() throws IOException {
    super.close(); // this method does stuff, make sure it's run!
    ceph.shutdown();
  }

  /**
   * Get an FSDataOutputStream to append onto a file.
   * @param path The File you want to append onto
   * @param bufferSize Ceph does internal buffering but you can buffer in the Java code as well if you like.
   * @param progress The Progressable to report progress to.
   * Reporting is limited but exists.
   * @return An FSDataOutputStream that connects to the file on Ceph.
   * @throws IOException If the file cannot be found or appended to.
   */
  public FSDataOutputStream append(Path path, int bufferSize,
      Progressable progress) throws IOException {
    path = makeAbsolute(path);

    if (progress != null) {
      progress.progress();
    }

    int fd = ceph.open(path, CephMount.O_WRONLY|CephMount.O_APPEND, 0);

    if (progress != null) {
      progress.progress();
    }

    CephOutputStream ostream = new CephOutputStream(getConf(), ceph, fd,
        bufferSize);
    return new FSDataOutputStream(ostream, statistics);
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

    boolean result = false;
    try {
      ceph.mkdirs(path, (int) perms.toShort());
      result = true;
    } catch (CephFileAlreadyExistsException e) {
      result = true;
    }

    return result;
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
          ceph.get_file_replication(path), stat.blksize, stat.m_time,
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

    String[] dirlist = ceph.listdir(path);
    if (dirlist != null) {
      FileStatus[] status = new FileStatus[dirlist.length];
      for (int i = 0; i < status.length; i++) {
        status[i] = getFileStatus(new Path(path, dirlist[i]));
      }
      return status;
    }

    if (isFile(path))
      return new FileStatus[] { getFileStatus(path) };

    return null;
  }

  /** {@inheritDocs} */
  @Override
  public void setPermission(Path path, FsPermission permission) throws IOException {
    path = makeAbsolute(path);
    ceph.chmod(path, permission.toShort());
  }

  /** {@inheritDocs} */
  @Override
  public void setTimes(Path path, long mtime, long atime) throws IOException {
    path = makeAbsolute(path);

    CephStat stat = new CephStat();
    int mask = 0;

    if (mtime != -1) {
      mask |= CephMount.SETATTR_MTIME;
      stat.m_time = mtime;
    }

    if (atime != -1) {
      mask |= CephMount.SETATTR_ATIME;
      stat.a_time = atime;
    }

    ceph.setattr(path, stat, mask);
  }

  /**
   * Get data pools from configuration.
   *
   * Package-private: used by unit tests
   */
  String[] getConfiguredDataPools() {
    String pool_list = getConf().get(
        CephConfigKeys.CEPH_DATA_POOLS_KEY,
        CephConfigKeys.CEPH_DATA_POOLS_DEFAULT);

    if (pool_list != null)
      return pool_list.split(",");

    return new String[0];
  }

  /**
   * Lookup pool size by name.
   *
   * Package-private: used by unit tests
   */
  int getPoolReplication(String pool_name) throws IOException {
    int pool_id = ceph.get_pool_id(pool_name);
    return ceph.get_pool_replication(pool_id);
  }

  /**
   * Select a data pool given the requested replication factor.
   */
  private String selectDataPool(Path path, int repl_wanted) throws IOException {
    /* map pool size -> pool name */
    TreeMap<Integer, String> pools = new TreeMap<Integer, String>();

    /*
     * Start with a mapping for the default pool. An error here would indicate
     * something bad, so we throw any exceptions. For configured pools we
     * ignore some errors.
     */
    int fd = ceph.__open(new Path("/"), CephMount.O_RDONLY, 0);
    String pool_name = ceph.get_file_pool_name(fd);
    ceph.close(fd);
    int replication = getPoolReplication(pool_name);
    pools.put(new Integer(replication), pool_name);

    /*
     * Insert extra data pools from configuration. Errors are logged (most
     * likely a non-existant pool), and a configured pool will override the
     * default pool.
     */
    String[] conf_pools = getConfiguredDataPools();
    for (String name : conf_pools) {
      try {
        replication = getPoolReplication(name);
        pools.put(new Integer(replication), name);
      } catch (IOException e) {
        LOG.warn("Error looking up replication of pool: " + name + ", " + e);
      }
    }

    /* Choose smallest entry >= target, or largest in map. */
    Map.Entry<Integer, String> entry = pools.ceilingEntry(new Integer(repl_wanted));
    if (entry == null)
      entry = pools.lastEntry();

    /* should always contain default pool */
    assert(entry != null);

    replication = entry.getKey().intValue();
    pool_name = entry.getValue();

    /* log non-exact match cases */
    if (replication != repl_wanted) {
      LOG.info("selectDataPool path=" + path + " pool:repl=" +
          pool_name + ":" + replication + " wanted=" + repl_wanted);
    }

    return pool_name;
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

    int flags = CephMount.O_WRONLY | CephMount.O_CREAT;

    if (exists) {
      if (overwrite)
        flags |= CephMount.O_TRUNC;
      else
        throw new FileAlreadyExistsException();
    } else {
      Path parent = path.getParent();
      if (parent != null)
        if (!mkdirs(parent, permission))
          throw new IOException("mkdirs failed for " + parent.toString());
    }

    if (progress != null) {
      progress.progress();
    }

    /* Sanity check. Ceph interface uses int for striping strategy */
    if (blockSize > Integer.MAX_VALUE) {
      blockSize = Integer.MAX_VALUE;
      LOG.info("blockSize too large. Rounding down to " + blockSize);
    }

    /*
     * If blockSize <= 0 then we complain. We need to explicitly check for the
     * < 0 case (as opposed to allowing Ceph to raise an exception) because
     * the ceph_open_layout interface accepts -1 to request Ceph-specific
     * defaults.
     */
    if (blockSize <= 0)
      throw new IllegalArgumentException("Invalid block size: " + blockSize);

    /*
     * Ceph may impose alignment restrictions on file layout. In this case we
     * check if the requested block size is aligned to the granularity of a
     * stripe unit used in the file system. When the block size is not aligned
     * we automatically adjust to the next largest multiple of stripe unit
     * granularity.
     */
    int su = ceph.get_stripe_unit_granularity();
    if (blockSize % su != 0) {
      long newBlockSize = blockSize - (blockSize % su) + su;
      LOG.debug("fix alignment: blksize " + blockSize + " new blksize " + newBlockSize);
      blockSize = newBlockSize;
    }

    /*
     * The default Ceph data pool is selected to store files unless a specific
     * data pool is provided when a file is created. Since a pool has a fixed
     * replication factor, in order to achieve a requested replication factor,
     * we must select an appropriate data pool to place the file into.
     */
    String datapool = selectDataPool(path, replication);
    int fd = ceph.open(path, flags, (int)permission.toShort(), (int)blockSize,
        CEPH_STRIPE_COUNT, (int)blockSize, datapool);

    if (progress != null) {
      progress.progress();
    }

    OutputStream ostream = new CephOutputStream(getConf(), ceph, fd,
        bufferSize);
    return new FSDataOutputStream(ostream, statistics);
  }

  /**
  * Opens an FSDataOutputStream at the indicated Path with write-progress
  * reporting. Same as create(), except fails if parent directory doesn't
  * already exist.
  * @param f the file name to open
  * @param permission
  * @param overwrite if a file with this name already exists, then if true,
  * the file will be overwritten, and if false an error will be thrown.
  * @param bufferSize the size of the buffer to be used.
  * @param replication required block replication for the file.
  * @param blockSize
  * @param progress
  * @throws IOException
  * @see #setPermission(Path, FsPermission)
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
    src = makeAbsolute(src);
    dst = makeAbsolute(dst);

    try {
      CephStat stat = new CephStat();
      ceph.lstat(dst, stat);
      if (stat.isDir())
        return rename(src, new Path(dst, src.getName()));
      return false;
    } catch (FileNotFoundException e) {}

    try {
      ceph.rename(src, dst);
    } catch (FileNotFoundException e) {
      return false;
    }

    return true;
  }

  /**
   * Get a BlockLocation object for each block in a file.
   *
   * @param file A FileStatus object corresponding to the file you want locations for.
   * @param start The offset of the first part of the file you are interested in.
   * @param len The amount of the file past the offset you are interested in.
   * @return A BlockLocation[] where each object corresponds to a block within
   * the given range.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    Path abs_path = makeAbsolute(file.getPath());

    int fh = ceph.open(abs_path, CephMount.O_RDONLY, 0);
    if (fh < 0) {
      LOG.error("getFileBlockLocations:got error " + fh + ", exiting and returning null!");
      return null;
    }

    ArrayList<BlockLocation> blocks = new ArrayList<BlockLocation>();

    long curPos = start;
    long endOff = curPos + len;
    do {
      CephFileExtent extent = ceph.get_file_extent(fh, curPos);

      int[] osds = extent.getOSDs();
      String[] names = new String[osds.length];
      String[] hosts = new String[osds.length];
      String[] racks = new String[osds.length];

      for (int i = 0; i < osds.length; i++) {
        InetAddress addr = ceph.get_osd_address(osds[i]);
        names[i] = addr.getHostAddress();

        /*
         * Grab the hostname and rack from the crush hierarchy. Current we
         * hard code the item types. For a more general treatment, we'll need
         * a new configuration option that allows users to map their custom
         * crush types to hosts and topology.
         */
        Bucket[] path = ceph.get_osd_crush_location(osds[i]);
        for (Bucket bucket : path) {
          String type = bucket.getType();
          if (type.compareTo("host") == 0)
            hosts[i] = bucket.getName();
          else if (type.compareTo("rack") == 0)
            racks[i] = bucket.getName();
        }
      }

      blocks.add(new BlockLocation(names, hosts, racks,
            extent.getOffset(), extent.getLength()));

      curPos += extent.getLength();
    } while(curPos < endOff);

    ceph.close(fh);

    BlockLocation[] locations = new BlockLocation[blocks.size()];
    locations = blocks.toArray(locations);

    return locations;
  }

  @Deprecated
	public boolean delete(Path path) throws IOException {
		return delete(path, false);
	}

  /** {@inheritDoc} */
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
    if (!status.isDir()) {
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

    ceph.rmdir(path);
    return true;
  }

  @Override
  public short getDefaultReplication() {
    return ceph.getDefaultReplication();
  }

  @Override
  public long getDefaultBlockSize() {
    return getConf().getLong(
        CephConfigKeys.CEPH_OBJECT_SIZE_KEY,
        CephConfigKeys.CEPH_OBJECT_SIZE_DEFAULT);
  }

}
