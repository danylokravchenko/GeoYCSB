/**
 * Copyright (c) 2016 Yahoo! Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.couchbase2;

import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.env.resources.IoPoolShutdownHook;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.metrics.DefaultLatencyMetricsCollectorConfig;
import com.couchbase.client.core.metrics.DefaultMetricsCollectorConfig;
import com.couchbase.client.core.metrics.LatencyMetricsCollectorConfig;
import com.couchbase.client.core.metrics.MetricsCollectorConfig;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonFactory;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonGenerator;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.deps.io.netty.channel.DefaultSelectStrategyFactory;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.SelectStrategy;
import com.couchbase.client.deps.io.netty.channel.SelectStrategyFactory;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.deps.io.netty.util.IntSupplier;
import com.couchbase.client.deps.io.netty.util.concurrent.DefaultThreadFactory;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.queries.GeoDistanceQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.transcoder.JacksonTransformers;
import com.couchbase.client.java.util.Blocking;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.SpatialViewResult;
import org.json.JSONObject;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.GeoDB;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.io.StringWriter;
import java.io.Writer;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import site.ycsb.generator.GeoGenerator;
import site.ycsb.workloads.GeoWorkload;

/**
 * A class that wraps the 2.x Couchbase SDK to be used with YCSB.
 *
 * <p> The following options can be passed when using this database client to override the defaults.
 *
 * <ul>
 * <li><b>couchbase.host=127.0.0.1</b> The hostname from one server.</li>
 * <li><b>couchbase.bucket=default</b> The bucket name to use.</li>
 * <li><b>couchbase.password=</b> The password of the bucket.</li>
 * <li><b>couchbase.syncMutationResponse=true</b> If mutations should wait for the response to complete.</li>
 * <li><b>couchbase.persistTo=0</b> Persistence durability requirement</li>
 * <li><b>couchbase.replicateTo=0</b> Replication durability requirement</li>
 * <li><b>couchbase.upsert=false</b> Use upsert instead of insert or replace.</li>
 * <li><b>couchbase.adhoc=false</b> If set to true, prepared statements are not used.</li>
 * <li><b>couchbase.kv=true</b> If set to false, mutation operations will also be performed through N1QL.</li>
 * <li><b>couchbase.maxParallelism=1</b> The server parallelism for all n1ql queries.</li>
 * <li><b>couchbase.kvEndpoints=1</b> The number of KV sockets to open per server.</li>
 * <li><b>couchbase.queryEndpoints=5</b> The number of N1QL Query sockets to open per server.</li>
 * <li><b>couchbase.epoll=false</b> If Epoll instead of NIO should be used (only available for linux.</li>
 * <li><b>couchbase.boost=3</b> If > 0 trades CPU for higher throughput. N is the number of event loops, ideally
 *      set to the number of physical cores. Setting higher than that will likely degrade performance.</li>
 * <li><b>couchbase.networkMetricsInterval=0</b> The interval in seconds when latency metrics will be logged.</li>
 * <li><b>couchbase.runtimeMetricsInterval=0</b> The interval in seconds when runtime metrics will be logged.</li>
 * <li><b>couchbase.documentExpiry=0</b> Document Expiry is the amount of time until a document expires in
 *      Couchbase.</li>
 * </ul>
 */
public class Couchbase2Client extends GeoDB {

  private static final String SEPARATOR = ":";
  private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Couchbase2Client.class);
  private static final Object INIT_COORDINATOR = new Object();
  private static volatile CouchbaseEnvironment env = null;

  static {
    // No need to send the full encoded_plan for this benchmark workload, less network overhead!
    System.setProperty("com.couchbase.query.encodedPlanEnabled", "false");
  }

  private Cluster cluster;
  private Bucket bucket;
  private String bucketName;
  private boolean upsert;
  private PersistTo persistTo;
  private ReplicateTo replicateTo;
  private boolean syncMutResponse;
  private boolean epoll;
  private long kvTimeout;
  private boolean adhoc;
  private boolean kv;
  private int maxParallelism;
  private String host;
  private int kvEndpoints;
  private int queryEndpoints;
  private int boost;
  private int networkMetricsInterval;
  private int runtimeMetricsInterval;
  private String scanAllQuery;
  private String geoInsertN1qlQuery;
  private int documentExpiry;

  /**
   * Helper method to turn the values into a String, used with {@link #upsertN1ql(String, Map)}.
   *
   * @param values the values to encode.
   * @return the encoded string.
   */
  private static String encodeN1qlFields(final Map<String, ByteIterator> values) {
    if (values.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String raw = entry.getValue().toString();
      String escaped = raw.replace("\"", "\\\"").replace("'", "\\'");
      sb.append(entry.getKey()).append("=\"").append(escaped).append("\" ");
    }
    String toReturn = sb.toString();
    return toReturn.substring(0, toReturn.length() - 1);
  }

  /**
   * Helper method to turn the map of values into a {@link JsonObject} for further use.
   *
   * @param values the values to transform.
   * @return the created json object.
   */
  private static JsonObject valuesToJsonObject(final Map<String, ByteIterator> values) {
    JsonObject result = JsonObject.create();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      result.put(entry.getKey(), entry.getValue().toString());
    }
    return result;
  }

  /**
   * Helper method to join the set of fields into a String suitable for N1QL.
   *
   * @param fields the fields to join.
   * @return the joined fields as a String.
   */
  private static String joinFields(final Set<String> fields) {
    if (fields == null || fields.isEmpty()) {
      return "*";
    }
    StringBuilder builder = new StringBuilder();
    for (String f : fields) {
      builder.append("`").append(f).append("`").append(",");
    }
    String toReturn = builder.toString();
    return toReturn.substring(0, toReturn.length() - 1);
  }

  /**
   * Helper method to turn the prefix and key into a proper document ID.
   *
   * @param prefix the prefix (table).
   * @param key    the key itself.
   * @return a document ID that can be used with Couchbase.
   */
  private static String formatId(final String prefix, final String key) {
    return prefix + SEPARATOR + key;
  }

  /**
   * Helper method to parse the "ReplicateTo" property on startup.
   *
   * @param property the proeprty to parse.
   * @return the parsed setting.
   */
  private static ReplicateTo parseReplicateTo(final String property) throws
      DBException {
    int value = Integer.parseInt(property);

    switch (value) {
    case 0:
      return ReplicateTo.NONE;
    case 1:
      return ReplicateTo.ONE;
    case 2:
      return ReplicateTo.TWO;
    case 3:
      return ReplicateTo.THREE;
    default:
      throw new DBException("\"couchbase.replicateTo\" must be between 0 and 3");
    }
  }

  /**
   * Helper method to parse the "PersistTo" property on startup.
   *
   * @param property the proeprty to parse.
   * @return the parsed setting.
   */
  private static PersistTo parsePersistTo(final String property) throws
      DBException {
    int value = Integer.parseInt(property);

    switch (value) {
    case 0:
      return PersistTo.NONE;
    case 1:
      return PersistTo.ONE;
    case 2:
      return PersistTo.TWO;
    case 3:
      return PersistTo.THREE;
    case 4:
      return PersistTo.FOUR;
    default:
      throw new DBException("\"couchbase.persistTo\" must be between 0 and 4");
    }
  }

  @Override
  public void init() throws
      DBException {
    Properties props = getProperties();

    host = props.getProperty("couchbase.host", "127.0.0.1");
    bucketName = props.getProperty("couchbase.bucket", "default");
    String bucketPassword = props.getProperty("couchbase.password", "");

    upsert = props.getProperty("couchbase.upsert", "false").equals("true");
    persistTo = parsePersistTo(props.getProperty("couchbase.persistTo", "0"));
    replicateTo = parseReplicateTo(props.getProperty("couchbase.replicateTo", "0"));
    syncMutResponse = props.getProperty("couchbase.syncMutationResponse", "true").equals("true");
    adhoc = props.getProperty("couchbase.adhoc", "false").equals("true");
    kv = props.getProperty("couchbase.kv", "true").equals("true");
    maxParallelism = Integer.parseInt(props.getProperty("couchbase.maxParallelism", "1"));
    kvEndpoints = Integer.parseInt(props.getProperty("couchbase.kvEndpoints", "1"));
    queryEndpoints = Integer.parseInt(props.getProperty("couchbase.queryEndpoints", "1"));
    epoll = props.getProperty("couchbase.epoll", "false").equals("true");
    boost = Integer.parseInt(props.getProperty("couchbase.boost", "3"));
    networkMetricsInterval = Integer.parseInt(props.getProperty("couchbase.networkMetricsInterval", "0"));
    runtimeMetricsInterval = Integer.parseInt(props.getProperty("couchbase.runtimeMetricsInterval", "0"));
    documentExpiry = Integer.parseInt(props.getProperty("couchbase.documentExpiry", "0"));
    scanAllQuery = "SELECT RAW meta().id FROM `" + bucketName + "` WHERE meta().id >= '$1' ORDER BY meta().id LIMIT $2";
    geoInsertN1qlQuery = "INSERT INTO `" + bucketName + "`(KEY,VALUE) VALUES ($1,$2)";

    try {
      synchronized (INIT_COORDINATOR) {
        if (env == null) {

          LatencyMetricsCollectorConfig latencyConfig =
              networkMetricsInterval <= 0 ? DefaultLatencyMetricsCollectorConfig.disabled() :
                  DefaultLatencyMetricsCollectorConfig.builder().emitFrequency(networkMetricsInterval)
                      .emitFrequencyUnit(TimeUnit.SECONDS).build();

          MetricsCollectorConfig runtimeConfig =
              runtimeMetricsInterval <= 0 ? DefaultMetricsCollectorConfig.disabled() :
                  DefaultMetricsCollectorConfig.create(runtimeMetricsInterval, TimeUnit.SECONDS);

          DefaultCouchbaseEnvironment.Builder builder =
              DefaultCouchbaseEnvironment.builder().queryEndpoints(queryEndpoints).callbacksOnIoPool(true)
                  .runtimeMetricsCollectorConfig(runtimeConfig).networkLatencyMetricsCollectorConfig(latencyConfig)
                  .socketConnectTimeout(10000) // 10 secs socket connect timeout
                  .connectTimeout(30000) // 30 secs overall bucket open timeout
                  .kvTimeout(10000) // 10 instead of 2.5s for KV ops
                  .kvEndpoints(kvEndpoints);

          // Tune boosting and epoll based on settings
          SelectStrategyFactory factory =
              boost > 0 ? new BackoffSelectStrategyFactory() : DefaultSelectStrategyFactory.INSTANCE;

          int poolSize = boost > 0 ? boost : Integer.parseInt(
              System.getProperty("com.couchbase.ioPoolSize", Integer.toString(DefaultCoreEnvironment.IO_POOL_SIZE)));
          ThreadFactory threadFactory = new DefaultThreadFactory("cb-io", true);

          EventLoopGroup group = epoll ? new EpollEventLoopGroup(poolSize, threadFactory, factory) :
              new NioEventLoopGroup(poolSize, threadFactory, SelectorProvider.provider(), factory);
          builder.ioPool(group, new IoPoolShutdownHook(group));

          env = builder.build();
          logParams();
        }
      }

      cluster = CouchbaseCluster.create(env, host);
      bucket = cluster.openBucket(bucketName, bucketPassword);
      kvTimeout = env.kvTimeout();
    } catch (Exception ex) {
      throw new DBException("Could not connect to Couchbase Bucket.", ex);
    }

    if (!kv && !syncMutResponse) {
      throw new DBException("Not waiting for N1QL responses on mutations not yet implemented.");
    }
  }

  /**
   * Helper method to log the CLI params so that on the command line debugging is easier.
   */
  private void logParams() {

    String sb = "host=" + host + ", bucket=" + bucketName + ", upsert=" + upsert + ", persistTo=" + persistTo +
        ", replicateTo=" + replicateTo + ", syncMutResponse=" + syncMutResponse + ", adhoc=" + adhoc + ", kv=" + kv +
        ", maxParallelism=" + maxParallelism + ", queryEndpoints=" + queryEndpoints + ", kvEndpoints=" + kvEndpoints +
        ", queryEndpoints=" + queryEndpoints + ", epoll=" + epoll + ", boost=" + boost + ", networkMetricsInterval=" +
        networkMetricsInterval + ", runtimeMetricsInterval=" + runtimeMetricsInterval;

    LOGGER.info("===> Using Params: " + sb);
  }

  @Override
  public Status read(final String table, final String key, Set<String> fields, final Map<String, ByteIterator> result) {
    try {
      String docId = formatId(table, key);
      if (kv) {
        return readKv(docId, fields, result);
      } else {
        return readN1ql(docId, fields, result);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Performs the {@link #read(String, String, Set, Map)} operation via Key/Value ("get").
   *
   * @param docId  the document ID
   * @param fields the fields to be loaded
   * @param result the result map where the doc needs to be converted into
   * @return The result of the operation.
   */
  private Status readKv(final String docId, final Set<String> fields, final Map<String, ByteIterator> result) throws
      Exception {
    RawJsonDocument loaded = bucket.get(docId, RawJsonDocument.class);
    if (loaded == null) {
      return Status.NOT_FOUND;
    }
    decode(loaded.content(), fields, result);
    return Status.OK;
  }

  /**
   * Performs the {@link #read(String, String, Set, Map)} operation via N1QL ("SELECT").
   * <p>
   * If this option should be used, the "-p couchbase.kv=false" property must be set.
   *
   * @param docId  the document ID
   * @param fields the fields to be loaded
   * @param result the result map where the doc needs to be converted into
   * @return The result of the operation.
   */
  private Status readN1ql(final String docId, Set<String> fields, final Map<String, ByteIterator> result) throws
      Exception {
    String readQuery = "SELECT " + joinFields(fields) + " FROM `" + bucketName + "` USE KEYS [$1]";
    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(readQuery, JsonArray.from(docId),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException(
          "Error while parsing N1QL Result. Query: " + readQuery + ", Errors: " + queryResult.errors());
    }

    N1qlQueryRow row;
    try {
      row = queryResult.rows().next();
    } catch (NoSuchElementException ex) {
      return Status.NOT_FOUND;
    }

    JsonObject content = row.value();
    if (fields == null) {
      content = content.getObject(bucketName); // n1ql result set scoped under *.bucketName
      fields = content.getNames();
    }

    for (String field : fields) {
      Object value = content.get(field);
      result.put(field, new StringByteIterator(value != null ? value.toString() : ""));
    }

    return Status.OK;
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    if (upsert) {
      return upsert(table, key, values);
    }

    try {
      String docId = formatId(table, key);
      if (kv) {
        return updateKv(docId, values);
      } else {
        return updateN1ql(docId, values);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Performs the {@link #update(String, String, Map)} operation via Key/Value ("replace").
   *
   * @param docId  the document ID
   * @param values the values to update the document with.
   * @return The result of the operation.
   */
  private Status updateKv(final String docId, final Map<String, ByteIterator> values) {
    waitForMutationResponse(
        bucket.async().replace(RawJsonDocument.create(docId, documentExpiry, encode(values)), persistTo, replicateTo));
    return Status.OK;
  }

  /**
   * Performs the {@link #update(String, String, Map)} operation via N1QL ("UPDATE").
   * <p>
   * If this option should be used, the "-p couchbase.kv=false" property must be set.
   *
   * @param docId  the document ID
   * @param values the values to update the document with.
   * @return The result of the operation.
   */
  private Status updateN1ql(final String docId, final Map<String, ByteIterator> values) throws
      Exception {
    String fields = encodeN1qlFields(values);
    String updateQuery = "UPDATE `" + bucketName + "` USE KEYS [$1] SET " + fields;

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(updateQuery, JsonArray.from(docId),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException(
          "Error while parsing N1QL Result. Query: " + updateQuery + ", Errors: " + queryResult.errors());
    }
    return Status.OK;
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    if (upsert) {
      return upsert(table, key, values);
    }

    try {
      String docId = formatId(table, key);
      if (kv) {
        return insertKv(docId, values);
      } else {
        return insertN1ql(docId, values);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Performs the {@link #insert(String, String, Map)} operation via Key/Value ("INSERT").
   * <p>
   * Note that during the "load" phase it makes sense to retry TMPFAILS (so that even if the server is
   * overloaded temporarily the ops will succeed eventually). The current code will retry TMPFAILs
   * for maximum of one minute and then bubble up the error.
   *
   * @param docId  the document ID
   * @param values the values to update the document with.
   * @return The result of the operation.
   */
  private Status insertKv(final String docId, final Map<String, ByteIterator> values) {
    int tries = 60; // roughly 60 seconds with the 1 second sleep, not 100% accurate.

    for (int i = 0; i < tries; i++) {
      try {
        waitForMutationResponse(bucket.async()
            .insert(RawJsonDocument.create(docId, documentExpiry, encode(values)), persistTo, replicateTo));
        return Status.OK;
      } catch (TemporaryFailureException ex) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while sleeping on TMPFAIL backoff.", ex);
        }
      }
    }

    throw new RuntimeException(
        "Still receiving TMPFAIL from the server after trying " + tries + " times. " + "Check your server.");
  }

  /**
   * Performs the {@link #insert(String, String, Map)} operation via N1QL ("INSERT").
   * <p>
   * If this option should be used, the "-p couchbase.kv=false" property must be set.
   *
   * @param docId  the document ID
   * @param values the values to update the document with.
   * @return The result of the operation.
   */
  private Status insertN1ql(final String docId, final Map<String, ByteIterator> values) throws
      Exception {
    String insertQuery = "INSERT INTO `" + bucketName + "`(KEY,VALUE) VALUES ($1,$2)";

    N1qlQueryResult queryResult = bucket.query(
        N1qlQuery.parameterized(insertQuery, JsonArray.from(docId, valuesToJsonObject(values)),
            N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException(
          "Error while parsing N1QL Result. Query: " + insertQuery + ", Errors: " + queryResult.errors());
    }
    return Status.OK;
  }

  /**
   * Performs an upsert instead of insert or update using either Key/Value or N1QL.
   * <p>
   * If this option should be used, the "-p couchbase.upsert=true" property must be set.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  private Status upsert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      String docId = formatId(table, key);
      if (kv) {
        return upsertKv(docId, values);
      } else {
        return upsertN1ql(docId, values);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Performs the {@link #upsert(String, String, Map)} operation via Key/Value ("upsert").
   * <p>
   * If this option should be used, the "-p couchbase.upsert=true" property must be set.
   *
   * @param docId  the document ID
   * @param values the values to update the document with.
   * @return The result of the operation.
   */
  private Status upsertKv(final String docId, final Map<String, ByteIterator> values) {
    waitForMutationResponse(
        bucket.async().upsert(RawJsonDocument.create(docId, documentExpiry, encode(values)), persistTo, replicateTo));
    return Status.OK;
  }

  /**
   * Performs the {@link #upsert(String, String, Map)} operation via N1QL ("UPSERT").
   * <p>
   * If this option should be used, the "-p couchbase.upsert=true -p couchbase.kv=false" properties must be set.
   *
   * @param docId  the document ID
   * @param values the values to update the document with.
   * @return The result of the operation.
   */
  private Status upsertN1ql(final String docId, final Map<String, ByteIterator> values) throws
      Exception {
    String upsertQuery = "UPSERT INTO `" + bucketName + "`(KEY,VALUE) VALUES ($1,$2)";

    N1qlQueryResult queryResult = bucket.query(
        N1qlQuery.parameterized(upsertQuery, JsonArray.from(docId, valuesToJsonObject(values)),
            N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException(
          "Error while parsing N1QL Result. Query: " + upsertQuery + ", Errors: " + queryResult.errors());
    }
    return Status.OK;
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      String docId = formatId(table, key);
      if (kv) {
        return deleteKv(docId);
      } else {
        return deleteN1ql(docId);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Performs the {@link #delete(String, String)} (String, String)} operation via Key/Value ("remove").
   *
   * @param docId the document ID.
   * @return The result of the operation.
   */
  private Status deleteKv(final String docId) {
    waitForMutationResponse(bucket.async().remove(docId, persistTo, replicateTo));
    return Status.OK;
  }

  /**
   * Performs the {@link #delete(String, String)} (String, String)} operation via N1QL ("DELETE").
   * <p>
   * If this option should be used, the "-p couchbase.kv=false" property must be set.
   *
   * @param docId the document ID.
   * @return The result of the operation.
   */
  private Status deleteN1ql(final String docId) throws
      Exception {
    String deleteQuery = "DELETE FROM `" + bucketName + "` USE KEYS [$1]";
    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(deleteQuery, JsonArray.from(docId),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException(
          "Error while parsing N1QL Result. Query: " + deleteQuery + ", Errors: " + queryResult.errors());
    }
    return Status.OK;
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    try {
      if (fields == null || fields.isEmpty()) {
        return scanAllFields(table, startkey, recordcount, result);
      } else {
        return scanSpecificFields(table, startkey, recordcount, fields, result);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Performs the {@link #scan(String, String, int, Set, Vector)} operation, optimized for all fields.
   * <p>
   * Since the full document bodies need to be loaded anyways, it makes sense to just grab the document IDs
   * from N1QL and then perform the bulk loading via KV for better performance. This is a usual pattern with
   * Couchbase and shows the benefits of using both N1QL and KV together.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  private Status scanAllFields(final String table, final String startkey, final int recordcount,
                               final Vector<HashMap<String, ByteIterator>> result) {
    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    bucket.async().query(N1qlQuery.parameterized(scanAllQuery, JsonArray.from(formatId(table, startkey), recordcount),
            N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism))).doOnNext(result1 -> {
                if (!result1.parseSuccess()) {
                  throw new RuntimeException(
                      "Error while parsing N1QL Result. Query: " + scanAllQuery + ", Errors: " + result1.errors());
                }
              }).flatMap((Func1<AsyncN1qlQueryResult, Observable<AsyncN1qlQueryRow>>) result12 -> result12.rows())
        .flatMap((Func1<AsyncN1qlQueryRow, Observable<RawJsonDocument>>) row -> {
            String id = new String(row.byteValue()).trim();
            return bucket.async().get(id.substring(1, id.length() - 1), RawJsonDocument.class);
          }).map(document -> {
              HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
              decode(document.content(), null, tuple);
              return tuple;
            }).toBlocking().forEach(tuple -> data.add(tuple));

    result.addAll(data);
    return Status.OK;
  }

  /**
   * Performs the {@link #scan(String, String, int, Set, Vector)} operation N1Ql only for a subset of the fields.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  private Status scanSpecificFields(final String table, final String startkey, final int recordcount,
                                    final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {
    String scanSpecQuery =
        "SELECT " + joinFields(fields) + " FROM `" + bucketName + "` WHERE meta().id >= '$1' LIMIT $2";
    N1qlQueryResult queryResult = bucket.query(
        N1qlQuery.parameterized(scanSpecQuery, JsonArray.from(formatId(table, startkey), recordcount),
            N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException(
          "Error while parsing N1QL Result. Query: " + scanSpecQuery + ", Errors: " + queryResult.errors());
    }

    boolean allFields = fields == null || fields.isEmpty();
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      JsonObject value = row.value();
      if (fields == null) {
        value = value.getObject(bucketName);
      }
      Set<String> f = allFields ? value.getNames() : fields;
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(f.size());
      for (String field : f) {
        tuple.put(field, new StringByteIterator(value.getString(field)));
      }
      result.add(tuple);
    }
    return Status.OK;
  }

  /**
   * Helper method to block on the response, depending on the property set.
   * <p>
   * By default, since YCSB is sync the code will always wait for the operation to complete. In some
   * cases it can be useful to just "drive load" and disable the waiting. Note that when the
   * "-p couchbase.syncMutationResponse=false" option is used, the measured results by YCSB can basically
   * be thrown away. Still helpful sometimes during load phases to speed them up :)
   *
   * @param input the async input observable.
   */
  private void waitForMutationResponse(final Observable<? extends Document<?>> input) {
    if (!syncMutResponse) {
      input.subscribe(new Subscriber<Document<?>>() {
        @Override
        public void onCompleted() {
        }

        @Override
        public void onError(Throwable e) {
        }

        @Override
        public void onNext(Document<?> document) {
        }
      });
    } else {
      Blocking.blockForSingle(input, kvTimeout, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Decode the String from server and pass it into the decoded destination.
   *
   * @param source the loaded object.
   * @param fields the fields to check.
   * @param dest   the result passed back to YCSB.
   */
  private void decode(final String source, final Set<String> fields, final Map<String, ByteIterator> dest) {
    try {
      JsonNode json = JacksonTransformers.MAPPER.readTree(source);
      boolean checkFields = fields != null && !fields.isEmpty();
      for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
        Map.Entry<String, JsonNode> jsonField = jsonFields.next();
        String name = jsonField.getKey();
        if (checkFields && !fields.contains(name)) {
          continue;
        }
        JsonNode jsonValue = jsonField.getValue();
        if (jsonValue != null && !jsonValue.isNull()) {
          dest.put(name, new StringByteIterator(jsonValue.asText()));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not decode JSON");
    }
  }

  /**
   * Encode the source into a String for storage.
   *
   * @param source the source value.
   * @return the encoded string.
   */
  private String encode(final Map<String, ByteIterator> source) {
    Map<String, String> stringMap = StringByteIterator.getStringMap(source);
    ObjectNode node = JacksonTransformers.MAPPER.createObjectNode();
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    try {
      JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer);
      JacksonTransformers.MAPPER.writeTree(jsonGenerator, node);
    } catch (Exception e) {
      throw new RuntimeException("Could not encode JSON value");
    }
    return writer.toString();
  }

  @Override
  public Status geoLoad(String table, GeoGenerator generator, Double recordCount) {
    try {
      String docId = generator.getDocIdRandom();
      RawJsonDocument doc = bucket.get(docId, RawJsonDocument.class);
      if (doc != null) {
        generator.putDocument(docId, doc.content());
      } else {
        System.err.println("Error getting document from DB: " + docId);
      }
      generator.buildGeoInsertDocument();
      int inserts = (int) Math.round(recordCount / Integer.parseInt(GeoWorkload.TOTAL_DOCS_DEFAULT)) - 1;
      for (double i = inserts; i > 0; i--) {
        HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
        geoInsert(table, cells, generator);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  // *********************  GEO Insert ********************************

  @Override
  public Status geoInsert(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      if (kv) {
        return geoInsertKv(gen);
      } else {
        return geoInsertN1ql(gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status geoInsertKv(GeoGenerator gen) {
    int tries = 60; // roughly 60 seconds with the 1 second sleep, not 100% accurate.
    for (int i = 0; i < tries; i++) {
      try {
        waitForMutationResponse(bucket.async().insert(
            RawJsonDocument.create(gen.getGeoPredicate().getDocid(), documentExpiry, gen.getGeoPredicate().getValue()),
            persistTo, replicateTo));
        return Status.OK;
      } catch (TemporaryFailureException ex) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while sleeping on TMPFAIL backoff.", ex);
        }
      }
    }
    throw new RuntimeException(
        "Still receiving TMPFAIL from the server after trying " + tries + " times. " + "Check your server.");
  }

  private Status geoInsertN1ql(GeoGenerator gen) throws
      Exception {

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(geoInsertN1qlQuery,
        JsonArray.from(gen.getGeoPredicate().getDocid(), JsonObject.fromJson(gen.getGeoPredicate().getValue())),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException(
          "Error while parsing N1QL Result. Query: " + geoInsertN1qlQuery + ", Errors: " + queryResult.errors());
    }
    return Status.OK;
  }

  // *********************  GEO Update ********************************

  @Override
  public Status geoUpdate(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      if (kv) {
        return geoUpdateKv(gen);
      } else {
        return geoUpdateN1ql(gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status geoUpdateKv(GeoGenerator gen) {
    waitForMutationResponse(bucket.async().replace(
        RawJsonDocument.create(gen.getDocIdWithDistribution(), documentExpiry,
            gen.getGeoPredicate().getNestedPredicateA().getValueA().toString()), persistTo, replicateTo));

    return Status.OK;
  }

  private Status geoUpdateN1ql(GeoGenerator gen) throws
      Exception {
    String updateQuery =
        "UPDATE `" + bucketName + "` USE KEYS [$1] SET " + gen.getGeoPredicate().getNestedPredicateA().getName() +
            " = $2";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(updateQuery,
        JsonArray.from(gen.getDocIdWithDistribution(), gen.getGeoPredicate().getNestedPredicateA().getValueA()),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  // *********************  GEO Centre Based ********************************
  @Override
  public Status geoNear(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      JSONObject nearFieldValue = gen.getGeoPredicate().getNestedPredicateA().getValueA();

      HashMap<String, Object> boxFields = new ObjectMapper().readValue(nearFieldValue.toString(), HashMap.class);
      ArrayList coords1 = ((ArrayList) boxFields.get("coordinates"));

      GeoDistanceQuery fts = SearchQuery.geoDistance((Double) coords1.get(0), (Double) coords1.get(1), "1000m");
      SearchQuery query = new SearchQuery("Index", fts);

      SearchQueryResult queryResult = bucket.query(query);

      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  // *********************  GEO Box Based ********************************
  @Override
  public Status geoBox(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      JSONObject boxFieldValue1 = gen.getGeoPredicate().getNestedPredicateA().getValueA();
      JSONObject boxFieldValue2 = gen.getGeoPredicate().getNestedPredicateB().getValueA();

      HashMap<String, Object> boxFields = new ObjectMapper().readValue(boxFieldValue1.toString(), HashMap.class);
      HashMap<String, Object> boxFields1 = new ObjectMapper().readValue(boxFieldValue2.toString(), HashMap.class);
      List<Double> rp = new ArrayList<>();
      List coords = (ArrayList) boxFields.get("coordinates");
      for (Object element : coords) {
        rp.add((Double) element);
      }
      ArrayList coords2 = ((ArrayList) boxFields1.get("coordinates"));
      for (Object element : coords2) {
        rp.add((Double) element);
      }

      SpatialViewQuery q =
          SpatialViewQuery.from("_design/dev_spatial", "SpatialView").startRange(JsonArray.from(rp.get(0), rp.get(1)))
              .endRange(JsonArray.from(rp.get(2), rp.get(3)));
      SpatialViewResult queryResult = bucket.query(q);
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  // *********************  GEO Intersect ********************************
  @Override
  public Status geoIntersect(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      JSONObject boxFieldValue1 = gen.getGeoPredicate().getNestedPredicateA().getValueA();
      JSONObject boxFieldValue2 = gen.getGeoPredicate().getNestedPredicateB().getValueA();
      HashMap<String, Object> boxFields = new ObjectMapper().readValue(boxFieldValue1.toString(), HashMap.class);
      HashMap<String, Object> boxFields1 = new ObjectMapper().readValue(boxFieldValue2.toString(), HashMap.class);
      List<Double> rp = new ArrayList<>();
      List coords = (ArrayList) boxFields.get("coordinates");
      for (Object element : coords) {
        rp.add((Double) element);
      }
      ArrayList coords2 = ((ArrayList) boxFields1.get("coordinates"));
      for (Object element : coords2) {
        rp.add((Double) element);
      }

      SpatialViewQuery q =
          SpatialViewQuery.from("_design/dev_spatial", "SpatialView").startRange(JsonArray.from(rp.get(0), rp.get(1)))
              .endRange(JsonArray.from(rp.get(2), rp.get(3)));
      SpatialViewResult queryResult = bucket.query(q);

      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  // ************************************************************************************************
}

/**
 * Factory for the {@link BackoffSelectStrategy} to be used with boosting.
 */
class BackoffSelectStrategyFactory implements SelectStrategyFactory {
  @Override
  public SelectStrategy newSelectStrategy() {
    return new BackoffSelectStrategy();
  }
}

/**
 * Custom IO select strategy which trades CPU for throughput, used with the boost setting.
 */
class BackoffSelectStrategy implements SelectStrategy {

  private int counter = 0;

  @Override
  public int calculateStrategy(final IntSupplier supplier, final boolean hasTasks) throws
      Exception {
    int selectNowResult = supplier.get();
    if (hasTasks || selectNowResult != 0) {
      counter = 0;
      return selectNowResult;
    }
    counter++;

    if (counter > 2000) {
      LockSupport.parkNanos(1);
    } else if (counter > 3000) {
      Thread.yield();
    } else if (counter > 4000) {
      LockSupport.parkNanos(1000);
    } else if (counter > 5000) {
      // defer to blocking select
      counter = 0;
      return SelectStrategy.SELECT;
    }

    return SelectStrategy.CONTINUE;
  }
}
