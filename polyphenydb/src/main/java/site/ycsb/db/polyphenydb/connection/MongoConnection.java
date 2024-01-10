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

package site.ycsb.db.polyphenydb.connection;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import lombok.AllArgsConstructor;
import org.bson.BsonDocument;

import java.util.Arrays;
import java.util.List;


/**
 * PolyphenyDB supports Mongo query language, however, the MongoDB protocol is proprietary.
 * It mimicries via HTTP.
 *
 * @author Danylo Kravchenko
 */
@AllArgsConstructor
public class MongoConnection {

  public static final String MONGO_PREFIX = "/mongo";
  public static final ObjectMapper MAPPER = new ObjectMapper() {
    {
      setSerializationInclusion(JsonInclude.Include.NON_NULL);
      configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
      writerWithDefaultPrettyPrinter();
    }
  };
  private String host;
  private String port;
  private String protocol;
  private String database;
  private String username;
  private String password;

  private static DocResult getBody(HttpResponse<String> res) {
    try {
      DocResult[] result = MAPPER.readValue(res.getBody(), DocResult[].class);
      if (result.length == 1) {
        if (result[0].getError() != null) {
          throw new RuntimeException(result[0].getError());
        }
        return result[0];
      } else if (result.length == 0) {
        return DocResult.builder().build();
      }
      return result[result.length - 1];

    } catch (JsonSyntaxException | JsonProcessingException e) {
      System.err.println(String.format("%s\nmessage: %s", res.getBody(), e.getMessage()));
      throw new RuntimeException("This cannot happen");
    }
  }

  public static String toDoc(String key, Object value) {
    return String.format("{\"%s\": %s}", key, value);
  }

  public static String document(String... entries) {
    return String.format("{ %s }", String.join(",", entries));
  }

  public static String string(String string) {
    return String.format("\"%s\"", string);
  }

  public static String kv(String key, Object value) {
    return String.format("%s : %s", key, value);
  }

  public static BsonDocument tryGetBson(String entry) {
    BsonDocument doc = null;
    try {
      doc = BsonDocument.parse(entry);
    } catch (Exception e) {
      // empty on purpose
    }
    return doc;
  }

  public void dropCollection(String collection) {
    executeGetResponse("db." + collection + ".drop()");
  }

  public void initCollection(String collection) {
    executeGetResponse("db.createCollection(" + collection + ")");
  }

  public void tearDown() {
    dropDatabase();
  }

  public DocResult execute(String doc) {
    return executeGetResponse(doc);
  }

  public void initDatabase() {
    executeGetResponse("use " + database);
  }

  public void insert(String json) {
    executeGetResponse("db." + database + ".insert(" + json + ")");
  }

  public void insertMany(List<String> jsons) {
    insertMany(jsons, database);
  }

  public void insertMany(List<String> jsons, String db) {
    executeGetResponse("db." + db + ".insertMany([" + String.join(",", jsons) + "])");
  }

  public void update(String query, String update) {
    update(query, update, database);
  }

  public void update(String query, String update, String db) {
    executeGetResponse("db." + db + ".update(" + query + ", " + update + ")");
  }

  public void deleteMany(String query) {
    executeGetResponse("db." + database + ".deleteMany(" + query + ")");
  }

  public void dropDatabase() {
    executeGetResponse("db.dropDatabase()");
  }

  public HttpRequest<?> buildQuery(String query) {
    JsonObject data = new JsonObject();
    data.addProperty("query", query);
    data.addProperty("namespace", database);
    return Unirest.post("{protocol}://{host}:{port}" + MONGO_PREFIX)
        .header("Content-ExpressionType", "application/json").body(data);
  }

  public HttpResponse<String> executeGet(String query) {
    HttpRequest<?> request = buildQuery(query);
    request.basicAuth(username, password);
    request.routeParam("protocol", protocol);
    request.routeParam("host", host);
    request.routeParam("port", port);
    return request.asString();
  }

  public DocResult executeGetResponse(String mongoQl) {
    return getBody(executeGet(mongoQl));
  }

  public void initCollection() {
    initCollection(database);
  }

  public void dropCollection() {
    dropCollection(database);
  }

  public void cleanDocuments() {
    deleteMany("{}");
  }

  public static String group(String doc) {
    return "{\"$group\":" + doc + "}";
  }

  public static String limit(Integer limit) {
    return "{\"$limit\":" + limit + "}";
  }

  public static String match(String doc) {
    return "{\"$match\":" + doc + "}";
  }

  public static String project(String doc) {
    return "{\"$project\":" + doc + "}";
  }

  public static String sort(String doc) {
    return "{\"$sort\":" + doc + "}";
  }

  public static String set(String doc) {
    return "{\"$set\":" + doc + "}";
  }

  public static String unset(String doc) {
    return "{\"$unset\":\"" + doc + "\"}";
  }

  public static String unset(List<String> docs) {
    return "{\"$unset\":[" + String.join(",", docs) + "]}";
  }

  public static String unwind(String path) {
    return "{\"$unwind\":\"" + path + "\"}";
  }

  public DocResult find(String query, String project) {
    return find(query, project, database);
  }

  public DocResult find(String query, String project, String db) {
    return executeGetResponse("db." + db + ".find(" + query + "," + project + ")");
  }

  public DocResult aggregate(String... stages) {
    return aggregate(database, Arrays.asList(stages));
  }

  public DocResult aggregate(String db, List<String> stages) {
    return executeGetResponse("db." + db + ".aggregate([" + String.join(",", stages) + "])");
  }

}
