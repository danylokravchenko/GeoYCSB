/**
 * Copyright (c) 2012 - 2024 YCSB contributors. All rights reserved.
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

package site.ycsb.db.polyphenydb;


import org.json.JSONObject;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.GeoDB;
import site.ycsb.Status;
import site.ycsb.db.polyphenydb.connection.DocResult;
import site.ycsb.db.polyphenydb.connection.MongoConnection;
import site.ycsb.generator.GeoGenerator;
import site.ycsb.workloads.GeoWorkload;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static site.ycsb.db.polyphenydb.connection.MongoConnection.*;

/**
 * PolyphenyDB binding for YCSB framework
 * See the <code>README.md</code> for configuration information.
 *
 * @author Danylo Kravchenko
 */
@SuppressWarnings("unused")
public class PolyphenyDbClient extends GeoDB {

  /**
   * Used to include a field in a response.
   */
  private static final Integer INCLUDE = 1;
  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static final String GEOMETRY = "$geometry";
  /**
   * The database name to access.
   */
  private static String databaseName;
  /**
   * A singleton Mongo instance.
   */
  private static MongoConnection mongoConnection;
  private Random rand;

  private static boolean containsResults(DocResult queryResult) {
    return queryResult.getData() != null && queryResult.getData().length != 0 && queryResult.getData()[0] != null;
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws
      DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      System.out.println("Ending session");
      mongoConnection = null;
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
      if (mongoConnection != null) {
        return;
      }

      Properties props = getProperties();

      String host = props.getProperty("polyphenydb.host", "127.0.0.1");
      String port = props.getProperty("polyphenydb.port", "13137");
      String protocol = props.getProperty("polyphenydb.protocol", "http");
      databaseName = props.getProperty("polyphenydb.database", "ycsb");
      String username = props.getProperty("polyphenydb.username", "pa");
      String password = props.getProperty("polyphenydb.password", "");

      String sb =
          "host=" + host + ", port=" + port + ", protocol=" + protocol + ", database=" + databaseName + ", username=" +
              username + ", password='******'";

      System.out.println("===> Using Params: " + sb);

      try {
        synchronized (INIT_COORDINATOR) {
          mongoConnection = new MongoConnection(host, port, protocol, databaseName, username, password);
          rand = new Random();
          importData();
        }
      } catch (Exception ex) {
        throw new DBException("Could not connect to PolyphenyDB.", ex);
      }
    }
  }

  private void importData() {
    mongoConnection.initDatabase();
    System.out.println("Initiated database " + databaseName);

    int batch = 64;
    int idx = 0;
    List<String> parsedResults = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(
        new FileReader("Graffiti_Abatement_IncidentsLine.json"))) {
      String line;
      while ((line = br.readLine()) != null) {
        // process the line.
        if (idx % batch == 0) {
          System.out.println("Inserting ... " + idx);
          mongoConnection.insertMany(parsedResults);
          parsedResults = new ArrayList<>();
        }
        parsedResults.add(line.trim());
        idx++;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return null;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return null;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return null;
  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }

  /*
       ================    GEO operations  ======================
  */

  @Override
  public Status geoLoad(String table, GeoGenerator generator, Double recordCount) {
    try {
      String key = generator.getDocIdRandom();
      int objId = rand.nextInt(
          (Integer.parseInt(GeoWorkload.TOTAL_DOCS_DEFAULT) - Integer.parseInt(GeoWorkload.DOCS_START_VALUE)) + 1) +
          Integer.parseInt(GeoWorkload.DOCS_START_VALUE);
      String query = document(kv(string("properties.OBJECTID"), objId));
      DocResult result = mongoConnection.find(query, document());
      String[] queryResult = result.getData();
      if (queryResult.length == 0) {
        System.out.println(table + " ++++ " + databaseName);
        System.out.println(query);
        System.out.println("Empty return");
        return Status.OK;
      }
      String data = queryResult[0];
      generator.putDocument(key, data);
      System.out.println("Key : " + key + " Query Result :" + data);
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

  @Override
  public Status geoInsert(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
//      String key = gen.getGeoPredicate().getDocid();
      String value = gen.getGeoPredicate().getValue();
//      BsonDocument doc = tryGetBson(value);
//      doc.entrySet().stream().filter(entry -> Objects.equals(entry.getKey(), "properties.OBJECTID")).collect(
//          Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      mongoConnection.insert(value);
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying insert");
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status geoUpdate(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      int key = rand.nextInt(
          (Integer.parseInt(GeoWorkload.TOTAL_DOCS_DEFAULT) - Integer.parseInt(GeoWorkload.DOCS_START_VALUE)) + 1) +
          Integer.parseInt(GeoWorkload.DOCS_START_VALUE);
      String updateFieldName = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject updateFieldValue = gen.getGeoPredicate().getNestedPredicateA().getValueA();
      String query = document(kv("properties.OBJECTID", key));
      String update = set(document(kv(updateFieldName, updateFieldValue)));
      mongoConnection.update(query, update);

      DocResult res = mongoConnection.find(query, document(kv(updateFieldName, 1)));
      if (res.getData() != null && !Objects.equals(res.getData()[0], updateFieldValue.toString())) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status geoNear(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      String nearFieldName = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject nearFieldValue = gen.getGeoPredicate().getNestedPredicateA().getValueA();
      DocResult queryResult = mongoConnection.find(document(kv(string(nearFieldName), document(
              kv(string("$near"), document(
                  kv(string(GEOMETRY), nearFieldValue), kv(string("$maxDistance"), 1000.0)))))),
          document());
      return containsResults(queryResult) ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status geoBox(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      String boxFieldName = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject boxFieldValue = gen.getGeoPredicate().getNestedPredicateD().getValueA();
      DocResult queryResult = mongoConnection.find(document(
              kv(string(boxFieldName), document(
                  kv(string("$geoWithin"), document(kv(string(GEOMETRY), boxFieldValue)))))),
          document());
      return containsResults(queryResult) ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status geoIntersect(String table, HashMap<String, ByteIterator> result, GeoGenerator gen) {
    try {
      String intersectFieldName = gen.getGeoPredicate().getNestedPredicateA().getName();
      JSONObject intersectFieldValue = gen.getGeoPredicate().getNestedPredicateD().getValueA();
      DocResult queryResult = mongoConnection.find(document(kv(string(intersectFieldName),
          document(kv(string("$geoIntersects"), document(kv(string(GEOMETRY), intersectFieldValue)))))), document());
      return containsResults(queryResult) ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

}
