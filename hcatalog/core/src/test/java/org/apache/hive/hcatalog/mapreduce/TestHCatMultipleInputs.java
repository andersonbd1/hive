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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.thrift.test.IntString;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Before;
import org.junit.Test;

public class TestHCatMultipleInputs extends HCatBaseTest {

  private boolean setUpComplete = false;

  private final String[] test_tables = {"testsequence", "testtext"};


  /**
   * Create an input sequence file, a table stored as text
   * with two partitions.
   */
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (setUpComplete) {
      return;
    }
    int cur_index = 0;

    Path intStringSeq = new Path(TEST_DATA_DIR + "/data/"+test_tables[cur_index]+"/intString.seq");
    LOG.info("Creating sequence file: " + intStringSeq);
    SequenceFile.Writer seqFileWriter = SequenceFile.createWriter(
      intStringSeq.getFileSystem(hiveConf), hiveConf, intStringSeq,
      NullWritable.class, BytesWritable.class);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TIOStreamTransport transport = new TIOStreamTransport(out);
    TBinaryProtocol protocol = new TBinaryProtocol(transport);

    for (int i = 1; i <= 100; i++) {
      out.reset();
      IntString intString = new IntString(cur_index, Integer.toString(i), i);
      intString.write(protocol);
      BytesWritable bytesWritable = new BytesWritable(out.toByteArray());
      seqFileWriter.append(NullWritable.get(), bytesWritable);
    }

    seqFileWriter.close();

    // Now let's load this file into a new Hive table.
    Assert.assertEquals(0, driver.run("drop table if exists "+test_tables[cur_index]).getResponseCode());
    Assert.assertEquals(0, driver.run(
      "create table "+ test_tables[cur_index] +" "+
        "row format serde 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer' " +
        "with serdeproperties ( " +
        "  'serialization.class'='org.apache.hadoop.hive.serde2.thrift.test.IntString', " +
        "  'serialization.format'='org.apache.thrift.protocol.TBinaryProtocol') " +
        "stored as" +
        "  inputformat 'org.apache.hadoop.mapred.SequenceFileInputFormat'" +
        "  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'")
      .getResponseCode());
    Assert.assertEquals(0, driver.run("load data local inpath '" + intStringSeq.getParent() +
      "' into table "+test_tables[cur_index]).getResponseCode());

    cur_index = 1;

    Path testTxt1 = new Path(TEST_DATA_DIR + "/data/"+test_tables[cur_index]+"/test1.txt");
    Path testTxt2 = new Path(TEST_DATA_DIR + "/data/"+test_tables[cur_index]+"/test2.txt");
    LOG.info("Creating text file: " + testTxt1);
    LOG.info("Creating text file: " + testTxt2);

    BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(testTxt1.getFileSystem(hiveConf).create(testTxt1)));
    BufferedWriter bw2 = new BufferedWriter(new OutputStreamWriter(testTxt2.getFileSystem(hiveConf).create(testTxt2)));

    for (int i = 1; i <= 100; i++) {
      String row = "";
      if (i % 2 == 0) {
        row = i+":"+1+"\n";
        bw1.append(row);
      } else {
        row = i+":"+2+"\n";
        bw2.append(row);
      }
    }
    bw1.close();
    bw2.close();


    Assert.assertEquals(0, driver.run("drop table if exists "+test_tables[cur_index]).getResponseCode());

    Assert.assertEquals(0, driver.run(
      "create table "+ test_tables[cur_index] +"(index int, other int) partitioned by (mod " +
              "string) row" +
              " format delimited fields terminated by ':' stored as textfile ")
      .getResponseCode());
    Assert.assertEquals(0, driver.run("load data local inpath '" + testTxt1 +
      "' into table "+test_tables[cur_index] + " partition (mod='1')").getResponseCode());
    Assert.assertEquals(0, driver.run("load data local inpath '" + testTxt2 +
      "' into table "+test_tables[cur_index] + " partition (mod='2')").getResponseCode());




    setUpComplete = true;
  }

  public Set<Integer> readFromPath() {
    Set<Integer> resultSet= new HashSet<Integer>();
    try{
      FileSystem fs = FileSystem.get(new Configuration());
      FileStatus[] status = fs.listStatus(new Path(TEST_DATA_DIR, "output"));
      for (int i=0;i<status.length;i++){
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
        String line;
        line=br.readLine();
        while (line != null){
          LOG.info("line: "+line);
          resultSet.add(Integer.parseInt(line));
          line=br.readLine();
        }
      }
    }catch(Exception e){
      e.printStackTrace();
      LOG.error(e.getMessage());
    }
    return resultSet;
  }


  @Test
  public void testRead() throws Exception {
    runJob();
    Set<Integer> expectedSet= new HashSet<Integer>();
    expectedSet.add(0);
    expectedSet.add(1);
    expectedSet.add(2);
    Assert.assertEquals(expectedSet, readFromPath());
  }

  private boolean runJob() throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf);
    job.setJarByClass(this.getClass());


    job.setOutputFormatClass(TextOutputFormat.class);

    HCatMultipleInputs.init(job);
    HCatMultipleInputs.addInput(test_tables[0], "default", null, SequenceMapper.class);
    HCatMultipleInputs.addInput(test_tables[1], null, "mod=\"1\"", TextMapper1.class);
    HCatMultipleInputs.addInput(test_tables[1], null, "mod=\"2\"", TextMapper2.class);
    HCatMultipleInputs.build();


    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DefaultHCatRecord.class);

    Path path = new Path(TEST_DATA_DIR, "output");
    if (path.getFileSystem(conf).exists(path)) {
      path.getFileSystem(conf).delete(path, true);
    }

    TextOutputFormat.setOutputPath(job, path);

    job.setReducerClass(MyReducer.class);


    return job.waitForCompletion(true);
  }

  public static class SequenceMapper extends Mapper<NullWritable, HCatRecord, Text, HCatRecord> {
    @Override
    public void map(NullWritable key, HCatRecord value, Context context)
      throws IOException, InterruptedException {

      LOG.info("Record: "+value);

      context.write(new Text(value.get(0).toString()), value);
    }
  }

  public static class TextMapper1 extends Mapper<WritableComparable, HCatRecord, Text, HCatRecord> {
    @Override
    public void map(WritableComparable key, HCatRecord value, Context context)
      throws IOException, InterruptedException {

      HCatSplit split = (HCatSplit) context.getInputSplit();

      LOG.info("Mapper 1");
      String mod = value.getString("mod", split.getTableSchema());
      LOG.info("Record: "+value);
      context.write(new Text(mod), value);
    }
  }

  public static class TextMapper2 extends Mapper<WritableComparable, HCatRecord, Text, HCatRecord> {
    @Override
    public void map(WritableComparable key, HCatRecord value, Context context)
      throws IOException, InterruptedException {

      HCatSplit split = (HCatSplit) context.getInputSplit();

      LOG.info("Mapper 2");
      String mod = value.getString("mod", split.getTableSchema());
      LOG.info("Record: "+value);
      context.write(new Text(mod), value);
    }
  }

  public static class MyReducer extends Reducer<Text, HCatRecord,NullWritable, Text> {

    @Override
    protected void reduce( Text key,
      java.lang.Iterable<HCatRecord> values,
      org.apache.hadoop.mapreduce.Reducer<Text, HCatRecord,
      NullWritable, Text>.Context context)
      throws IOException, InterruptedException {

      context.write(NullWritable.get(), key);
    }
  }
}
