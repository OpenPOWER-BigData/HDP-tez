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
package org.apache.tez.test;

import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;


public class NonRepeatableOutputCommitter extends OutputCommitter {

  public NonRepeatableOutputCommitter(OutputCommitterContext committerContext) {
    super(committerContext);
  }

  @Override
  public void initialize() throws Exception {
  }

  @Override
  public void setupOutput() throws Exception {
  }

  @Override
  public void commitOutput() throws Exception {
    // kill AM in the middle of committing at the first attempt
    if (getContext().getDAGAttemptNumber() == 1) {
      System.exit(-1);
    }
  }

  @Override
  public void abortOutput(
      org.apache.tez.dag.api.client.VertexStatus.State finalState)
      throws Exception {
  }

  @Override
  public boolean isCommitRepeatable() throws Exception {
    return false;
  }
}
