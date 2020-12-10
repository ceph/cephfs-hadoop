package org.apache.hadoop.fs.cephrgw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.AbstractFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * The CephRgw implementation of AbstractFileSystem.
 * This impl delegates to the old FileSystem
 */
public class CephRgw extends DelegateToFileSystem {
    /**
     * This constructor has the signature needed by
     * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
     *
     * @param theUri which must be that of localFs
     * @param conf
     * @throws IOException
     * @throws URISyntaxException
     */
    CephRgw(final URI theUri, final Configuration conf) throws IOException,
            URISyntaxException {
        super(theUri, new CephRgwFileSystem(conf), conf, "cephrgw", true);
    }
}
