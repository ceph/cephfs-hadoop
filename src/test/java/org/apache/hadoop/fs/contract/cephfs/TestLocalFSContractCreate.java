/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.cephfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.Test;

public class TestLocalFSContractCreate extends AbstractContractCreateTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
		System.out.println("-");
	  return new CephFSContract(conf);
  }

@Override
@Test
public void testCreateFileOverExistingFileNoOverwrite() throws Throwable {
	System.out.println("-sdfsdfs");

	super.testCreateFileOverExistingFileNoOverwrite();
}

@Override
@Test
public void testCreateNewFile() throws Throwable {
	System.out.println("-sdf");

	super.testCreateNewFile();
}

@Override
@Test
public void testCreatedFileIsImmediatelyVisible() throws Throwable {
	System.out.println("-ddd");
	// TODO Auto-generated method stub
	super.testCreatedFileIsImmediatelyVisible();
}

@Override
@Test
public void testOverwriteEmptyDirectory() throws Throwable {
	System.out.println("-s");

	// TODO Auto-generated method stub
	//super.testOverwriteEmptyDirectory();
}

@Override
@Test
public void testOverwriteExistingFile() throws Throwable {

	System.out.println("-a");
// TODO Auto-generated method stub
	super.testOverwriteExistingFile();
}

@Override
@Test
public void testOverwriteNonEmptyDirectory() throws Throwable {
	System.out.println("-0");
	// TODO Auto-generated method stub
	//super.testOverwriteNonEmptyDirectory();
}

}
