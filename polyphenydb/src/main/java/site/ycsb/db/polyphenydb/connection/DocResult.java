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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;

/**
 * PolyphenyDB MongoDB document result.
 * @author Danylo Kravchenko
 */
@EqualsAndHashCode()
@SuperBuilder(toBuilder = true)
@Value
@AllArgsConstructor
public class DocResult {

  /**
   * namespace type of result DOCUMENT/RELATIONAL.
   */
  @JsonProperty("dataModel")
  private DataModel dataModel;
  @JsonProperty("namespace")
  private String namespace;
  @JsonProperty("data")
  private String[] data;
  @JsonProperty("header")
  private FieldDefinition[] header;
  @JsonProperty("query")
  private String query;

  /**
   * Error message if a query failed.
   */
  @JsonProperty("error")
  private String error;

  /**
   * Number of affected rows.
   */
  @JsonProperty("affectedTuples")
  private long affectedTuples;

  public DocResult() {
    this.dataModel = DataModel.DOCUMENT;
    this.namespace = null;
    this.data = null;
    this.header = null;
    this.query = null;
    this.error = null;
    this.affectedTuples = 0;
  }

  /**
   * Data model kind.
   */
  @Getter
  public enum DataModel {
    RELATIONAL(1),
    DOCUMENT(2),
    GRAPH(3);

    // GRAPH, DOCUMENT, ...
    private final int id;


    DataModel(int id) {
      this.id = id;
    }

  }

  /**
   * Field definition of the {@link DocResult}.
   */
  @Value
  @NonFinal
  @AllArgsConstructor
  public static class FieldDefinition {

    @JsonProperty("name")
    private String name;
    // for both
    @JsonProperty("dataType")
    private String dataType; //varchar/int/etc

    public FieldDefinition() {
      this.name = null;
      this.dataType = null;
    }
  }

}
