/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
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

/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 */
package site.ycsb.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.util.JSON;

import java.util.Random;

import org.json.JSONObject;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.GeoDB;
import site.ycsb.Status;

import org.bson.Document;
import org.bson.types.Binary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import site.ycsb.StringByteIterator;
import site.ycsb.generator.GeoGenerator;
import site.ycsb.workloads.GeoWorkload;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc. <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 *
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public class MongoDbClient extends GeoDB {

  /** Used to include a field in a response. */
  private static final Integer INCLUDE = Integer.valueOf(1);

  /** The options to use for inserting many documents. */
  private static final InsertManyOptions INSERT_UNORDERED = new InsertManyOptions().ordered(false);

  /** The options to use for inserting a single document. */
  private static final UpdateOptions UPDATE_WITH_UPSERT = new UpdateOptions().upsert(true);
  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  /**
   * The database name to access.
   */
  private static String databaseName;
  /** The database name to access. */
  private static MongoDatabase database;
  /** A singleton Mongo instance. */
  private static MongoClient mongoClient;

  /** The default read preference for the test. */
  private static ReadPreference readPreference;

  /** The default write concern for the test. */
  private static WriteConcern writeConcern;

  /** The batch size to use for inserts. */
  private static int batchSize;

  /** If true then use updates with the upsert option for inserts. */
  private static boolean useUpsert;

  /** The bulk inserts pending for the thread. */
  private final List<Document> bulkInserts = new ArrayList<Document>();

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws
      DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        mongoClient.close();
      } catch (Exception e1) {
        System.err.println("Could not close MongoDB connection pool: " + e1);
        e1.printStackTrace();
      } finally {
        database = null;
        mongoClient = null;
      }
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link GeoDB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      DeleteResult result = collection.withWriteConcern(writeConcern).deleteOne(query);
      if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
        System.err.println("Nothing deleted for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws
      DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (INCLUDE) {
      if (mongoClient != null) {
        return;
      }

      Properties props = getProperties();

      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));

      // Set is inserts are done as upserts. Defaults to false.
      useUpsert = Boolean.parseBoolean(props.getProperty("mongodb.upsert", "false"));

      // Just use the standard connection format URL
      // http://docs.mongodb.org/manual/reference/connection-string/
      // to configure the client.
      String url = props.getProperty("mongodb.url", null);
      boolean defaultedUrl = false;
      if (url == null) {
        defaultedUrl = true;
        url = "mongodb://localhost:27017/ycsb?w=1";
      }

      url = OptionsSupport.updateUrl(url, props);

      if (!url.startsWith("mongodb://") && !url.startsWith("mongodb+srv://")) {
        System.err.println("ERROR: Invalid URL: '" + url + "'. Must be of the form " +
            "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options' " +
            "or 'mongodb+srv://<host>/database?options'. " +
            "http://docs.mongodb.org/manual/reference/connection-string/");
        System.exit(1);
      }

      try {
        MongoClientURI uri = new MongoClientURI(url);

        String uriDb = uri.getDatabase();
        if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty() && !"admin".equals(uriDb)) {
          databaseName = uriDb;
        } else {
          // If no database is specified in URI, use "ycsb"
          databaseName = "ycsb";
        }

        readPreference = uri.getOptions().getReadPreference();
        writeConcern = uri.getOptions().getWriteConcern();

        mongoClient = new MongoClient(uri);
        database =
            mongoClient.getDatabase(databaseName).withReadPreference(readPreference).withWriteConcern(writeConcern);

        System.out.println("mongo client connection created with " + url);
      } catch (Exception e1) {
        System.err.println("Could not initialize MongoDB connection pool for Loader: " + e1);
        e1.printStackTrace();
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link GeoDB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document toInsert = new Document("_id", key);
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        toInsert.put(entry.getKey(), entry.getValue().toArray());
      }

      if (batchSize == 1) {
        if (useUpsert) {
          // this is effectively an insert, but using an upsert instead due
          // to current inability of the framework to clean up after itself
          // between test runs.
          collection.replaceOne(new Document("_id", toInsert.get("_id")), toInsert, UPDATE_WITH_UPSERT);
        } else {
          collection.insertOne(toInsert);
        }
      } else {
        bulkInserts.add(toInsert);
        if (bulkInserts.size() == batchSize) {
          if (useUpsert) {
            List<UpdateOneModel<Document>> updates = new ArrayList<UpdateOneModel<Document>>(bulkInserts.size());
            for (Document doc : bulkInserts) {
              updates.add(new UpdateOneModel<Document>(new Document("_id", doc.get("_id")), doc, UPDATE_WITH_UPSERT));
            }
            collection.bulkWrite(updates);
          } else {
            collection.insertMany(bulkInserts, INSERT_UNORDERED);
          }
          bulkInserts.clear();
        } else {
          return Status.BATCHED_OK;
        }
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with " + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }

  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document("_id", key);

      FindIterable<Document> findIterable = collection.find(query);

      if (fields != null) {
        Document projection = new Document();
        for (String field : fields) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
      }

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        fillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link GeoDB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("_id", scanRange);
      Document sort = new Document("_id", INCLUDE);

      FindIterable<Document> findIterable = collection.find(query).sort(sort).limit(recordcount);

      if (fields != null) {
        Document projection = new Document();
        for (String fieldName : fields) {
          projection.put(fieldName, INCLUDE);
        }
        findIterable.projection(projection);
      }

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      Document fieldsToSet = new Document();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
      }
      Document update = new Document("$set", fieldsToSet);

      UpdateResult result = collection.updateOne(query, update);
      if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  /**
   * Fills the map with the values from the DBObject.
   *
   * @param resultMap
   *          The map to fill/
   * @param obj
   *          The object to copy values from.
   */
  protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      if (entry.getValue() instanceof Binary) {
        resultMap.put(entry.getKey(), new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
      }
    }
  }

  /*
       ================    GEO operations  ======================
   */

  @Override
  public Status geoLoad(String table, GeoGenerator generator, Double recordCount) {

    try {
      String key = generator.getDocIdRandom();
      MongoCollection<Document> collection = database.getCollection(table);
      Random rand = new Random();
      int objId = rand.nextInt(
          (Integer.parseInt(GeoWorkload.TOTAL_DOCS_DEFAULT) - Integer.parseInt(GeoWorkload.DOCS_START_VALUE)) + 1) +
          Integer.parseInt(GeoWorkload.DOCS_START_VALUE);
      Document query = new Document("properties.OBJECTID", objId);
      FindIterable<Document> findIterable = collection.find(query);
      Document queryResult = findIterable.first();
      if (queryResult == null) {
        System.out.println(table + " ++++ " + collection);
        System.out.println(query);
        System.out.println("Empty return");
        return Status.OK;
      }

      generator.putDocument(key, queryResult.toJson());
      System.out.println("Key : " + key + " Query Result :" + queryResult.toJson());
      generator.buildGeoInsertDocument();
      int inserts = (int) Math.round(recordCount / Integer.parseInt(GeoWorkload.TOTAL_DOCS_DEFAULT)) - 1;
      for (double i = inserts; i > 0; i--) {
        HashMap<String, ByteIterator> cells = new HashMap<>();
        geoInsert(table, cells, generator);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e);
    }
    return Status.ERROR;
  }

  // *********************  GEO Insert ********************************

  @Override
  public Status geoInsert(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {

    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String key = gen.getGeoPredicate().getDocid();
      String value = gen.getGeoPredicate().getValue();
      Document toInsert = new Document("OBJECTID", key);
      DBObject body = (DBObject) JSON.parse(value);
      toInsert.put(key, body);
      collection.insertOne(toInsert);

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with " + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  // *********************  GEO Update ********************************

  @Override
  public Status geoUpdate(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Random rand = new Random();
      int key = rand.nextInt(
          (Integer.parseInt(GeoWorkload.TOTAL_DOCS_DEFAULT) - Integer.parseInt(GeoWorkload.DOCS_START_VALUE)) + 1) +
          Integer.parseInt(GeoWorkload.DOCS_START_VALUE);
      String updateFieldName = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject updateFieldValue = gen.getGeoPredicate().getNestedPredicateA().getValueA();

      HashMap<String, Object> updateFields = new ObjectMapper().readValue(updateFieldValue.toString(), HashMap.class);
      Document refPoint = new Document(updateFields);
      Document query = new Document().append("properties.OBJECTID", key);
      Document fieldsToSet = new Document();

      fieldsToSet.put(updateFieldName, refPoint);
      Document update = new Document("$set", fieldsToSet);

      UpdateResult res = collection.updateMany(query, update);
      if (res.wasAcknowledged() && res.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  // *********************  GEO Near ********************************

  @Override
  public Status geoNear(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String nearFieldName = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject nearFieldValue = gen.getGeoPredicate().getNestedPredicateA().getValueA();

      HashMap<String, Object> nearFields = new ObjectMapper().readValue(nearFieldValue.toString(), HashMap.class);
      Document refPoint = new Document(nearFields);
      FindIterable<Document> findIterable = collection.find(Filters.near(nearFieldName, refPoint, 1000.0, 0.0));
      Document projection = new Document();
      for (String field : gen.getAllGeoFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        geoFillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }


  // *********************  GEO Box ********************************

  @Override
  public Status geoBox(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String boxFieldName1 = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject boxFieldValue1 = gen.getGeoPredicate().getNestedPredicateA().getValueA();
      JSONObject boxFieldValue2 = gen.getGeoPredicate().getNestedPredicateB().getValueA();

      HashMap<String, Object> boxFields = new ObjectMapper().readValue(boxFieldValue1.toString(), HashMap.class);
      Document refPoint = new Document();
      refPoint.putAll(boxFields);
      HashMap<String, Object> boxFields1 = new ObjectMapper().readValue(boxFieldValue2.toString(), HashMap.class);
      Document refPoint2 = new Document();
      refPoint2.putAll(boxFields1);
      ArrayList coords1 = ((ArrayList) refPoint.get("coordinates"));
      List<Double> rp = new ArrayList<>();
      for (Object element : coords1) {
        rp.add((Double) element);
      }
      ArrayList coords2 = ((ArrayList) refPoint2.get("coordinates"));
      for (Object element : coords2) {
        rp.add((Double) element);
      }

      FindIterable<Document> findIterable =
          collection.find(Filters.geoWithinBox(boxFieldName1, rp.get(0), rp.get(1), rp.get(2), rp.get(3)));
      Document projection = new Document();
      for (String field : gen.getAllGeoFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        geoFillMap(result, queryResult);
      }
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
      MongoCollection<Document> collection = database.getCollection(table);
      String fieldName1 = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject intersectFieldValue2 = gen.getGeoPredicate().getNestedPredicateC().getValueA();

      HashMap<String, Object> intersectFields =
          new ObjectMapper().readValue(intersectFieldValue2.toString(), HashMap.class);
      Document refPoint = new Document(intersectFields);
      FindIterable<Document> findIterable = collection.find(Filters.geoIntersects(fieldName1, refPoint));
      Document projection = new Document();
      for (String field : gen.getAllGeoFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        geoFillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  // *********************  GEO Scan ********************************
  @Override
  public Status geoScan(String table, final Vector<HashMap<String, ByteIterator>> result, GeoGenerator gen) {
    String startkey = gen.getDocIdWithDistribution();
    int recordcount = gen.getRandomLimit();
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("OBJECTID", scanRange);

      FindIterable<Document> findIterable = collection.find(query).limit(recordcount);

      Document projection = new Document();
      for (String field : gen.getAllGeoFields()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        geoFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  protected void geoFillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      String value = "null";
      if (entry.getValue() != null) {
        value = entry.getValue().toString();
      }
      resultMap.put(entry.getKey(), new StringByteIterator(value));
    }
  }
}
