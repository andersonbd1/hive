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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class supports MapReduce jobs that use multiple HCatalog
 * tables as input. Usage is similar to the MultipleInputs class,
 * which allows a user to specify a different InputFormat for each input Path.
 * <p/>
 * <p/>
 * Usage pattern for job submission:
 * <p/>
 * <pre>
 * Configuration conf = new Configuration();
 *
 * Job job = new Job(conf);
 * job.setJarByClass(this.getClass());
 *
 * job.setOutputFormatClass(TextOutputFormat.class);
 *
 * HCatMultipleInputs.init(job);
 * HCatMultipleInputs.addInput(test_table1, "default", null, SequenceMapper.class);
 * HCatMultipleInputs.addInput(test_table2, null, "part='1'", TextMapper1.class);
 * HCatMultipleInputs.addInput(test_table2, null, "part='2'", TextMapper2.class);
 * HCatMultipleInputs.build();
 *
 * job.setMapOutputKeyClass(Text.class);
 * job.setMapOutputValueClass(DefaultHCatRecord.class);
 *
 * Path path = new Path(TEST_DATA_DIR, "output");
 *
 * TextOutputFormat.setOutputPath(job, path);
 *
 * job.setReducerClass(MyReducer.class);
 *
 * return job.waitForCompletion(true);
 */

public class HCatMultipleInputs {

  private static final Logger LOGGER = LoggerFactory.getLogger(HCatMultipleInputs.class.getName());

  public static final String HCAT_KEY_MULTI_INPUT_JOBS_INFO = HCatConstants.HCAT_KEY_BASE + "" +
          ".multi.jobs.info";
  public static final String HCAT_KEY_MULTI_INPUT_JOBS_INFO_MAPPER = HCatConstants.HCAT_KEY_BASE +
          ".multi.jobs.info.mapper";

  public static final String HCAT_KEY_MULTI_INPUT_JOB_INFO_INDEX = HCatConstants.HCAT_KEY_BASE +
          ".multi.job.info.index";


  private static Job job;
  private static final ArrayList<InputJobInfo> inputJobInfoList = new ArrayList<InputJobInfo>();
  private static final ArrayList<String> inputJobMapperList = new ArrayList<String>();



  public static void init(Job _job) {
    job = _job;
    inputJobInfoList.clear();
    inputJobMapperList.clear();
  }

  public static void build() throws IOException {
    Configuration conf = job.getConfiguration();
    conf.set(HCAT_KEY_MULTI_INPUT_JOBS_INFO, HCatUtil.serialize(inputJobInfoList));
    conf.set(HCAT_KEY_MULTI_INPUT_JOBS_INFO_MAPPER, HCatUtil.serialize(inputJobMapperList));
    job.setInputFormatClass(HCatDelegatingInputFormat.class);
    job.setMapperClass(HCatDelegatingMapper.class);

  }


  /**
   * @param table       Table name to
   * @param dbName      The database that the table belongs to. If null, the default database will be used.
   * @param filter      The partition filter that should be applied to the table, can be null.
   * @param mapperClass The mapper that should be used for this input, not null.
   */

  @SuppressWarnings("rawtypes")
  public static void addInput(String table, String dbName, String filter, Class<? extends Mapper> mapperClass) throws Exception {
    String mapperClassStr = null;
    if (mapperClass != null) {
      mapperClassStr = mapperClass.getName();
    }
    addInput(table, dbName, filter, mapperClassStr);
  }

  @SuppressWarnings("rawtypes")
  public static void addInput(String table, String dbName, String filter,
                              String mapperClass) throws Exception {
    Configuration conf = job.getConfiguration();
    InputJobInfo info = InputJobInfo.create(dbName, table, filter, null);
    info = InitializeInput.getInputJobInfoForMultiInputs(conf, info, null);
    inputJobInfoList.add(info);
    inputJobMapperList.add(mapperClass);
  }

  static void writeInputJobInfoIndexToSplit(InputSplit split, int index) {
    try {
      writeInputJobInfoIndexToSplit(InternalUtil.castToHCatSplit(split), index);
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    }
  }

  static void writeInputJobInfoIndexToSplit(HCatSplit split, int index) {
    Properties props = split.getPartitionInfo().getInputStorageHandlerProperties();
    props.setProperty(HCatMultipleInputs.HCAT_KEY_MULTI_INPUT_JOB_INFO_INDEX, String.valueOf(index));
  }


  static InputJobInfo readInputJobInfoFromSplit(Configuration conf, InputSplit split) throws IOException {
    int index = readInputJobInfoIndexFromSplit(split);
    ArrayList<InputJobInfo> _inputJobInfoList = (ArrayList<InputJobInfo>) HCatUtil.deserialize
            (conf.get(HCatMultipleInputs.HCAT_KEY_MULTI_INPUT_JOBS_INFO));
    return _inputJobInfoList.get(index);
  }


  static String readInputJobMapperFromSplit(Configuration conf, InputSplit split) throws IOException {
    int index = readInputJobInfoIndexFromSplit(split);
    ArrayList<String> _inputJobMapperList = (ArrayList<String>) HCatUtil.deserialize
            (conf.get(HCAT_KEY_MULTI_INPUT_JOBS_INFO_MAPPER));
    return _inputJobMapperList.get(index);
  }

  static private int readInputJobInfoIndexFromSplit(InputSplit split) {
    try {
      return readInputJobInfoIndexFromSplit(InternalUtil.castToHCatSplit(split));
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    }
    return -1;
  }

  static private int readInputJobInfoIndexFromSplit(HCatSplit split) {

    return Integer.valueOf(split.getPartitionInfo().getInputStorageHandlerProperties().getProperty
            (HCatMultipleInputs.HCAT_KEY_MULTI_INPUT_JOB_INFO_INDEX));
  }
}
