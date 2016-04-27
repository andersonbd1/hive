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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;

class HCatDelegatingInputFormat extends HCatBaseInputFormat {

  private void setInput(Configuration conf, InputJobInfo info) throws IOException {
    conf.set(
            HCatConstants.HCAT_KEY_JOB_INFO,
            HCatUtil.serialize(info));
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
    throws IOException, InterruptedException {
    ArrayList<InputSplit> result = new ArrayList<InputSplit>();

    Configuration ctxConf = jobContext.getConfiguration();
    ArrayList<InputJobInfo> inputJobInfoList = (ArrayList<InputJobInfo>) HCatUtil.deserialize(ctxConf.get(HCatMultipleInputs.HCAT_KEY_MULTI_INPUT_JOBS_INFO));

    for (int index = 0; index < inputJobInfoList.size(); index ++) {
      InputJobInfo info = inputJobInfoList.get(index);
      Configuration conf = new Configuration(ctxConf);
      setInput(conf, info);
      JobContext ctx = ShimLoader.getHadoopShims().getHCatShim().createJobContext(conf, jobContext.getJobID());
      List<InputSplit> splits = super.getSplits(ctx);
      for (InputSplit split : splits) {
        HCatMultipleInputs.writeInputJobInfoIndexToSplit(split, index);
      }
      result.addAll(splits);
    }
    return result;
  }

  @Override
  public RecordReader<WritableComparable, HCatRecord> createRecordReader(
      InputSplit split, TaskAttemptContext taskContext) throws IOException, InterruptedException {

    InputJobInfo info = HCatMultipleInputs.readInputJobInfoFromSplit(taskContext.getConfiguration
            (), split);
    setInput(taskContext.getConfiguration(), info);

    return super.createRecordReader(split, taskContext);
  }

}

