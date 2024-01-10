package site.ycsb.generator;

import site.ycsb.workloads.GeoWorkload;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

import org.json.*;

/**
 * Author: Yuvraj Kanwar, adapted by Danylo Kravchenko
 * <p>
 * The storage-based generator is fetching pre-generated values/documents from an internal in-memory database instead
 * of generating new random values on the fly.
 * This approach allows YCSB to operate with real (or real-looking) JSON documents rather then synthetic.
 * <p>
 * It also provides the ability to query rich JSON documents by splitting JSON documents into query predicates
 * (field, value, type, field-value relation, logical operation)
 */
public abstract class GeoGenerator {


  public static final String GEO_DOCUMENT_PREFIX_COLLECTION = "usertable";
  public static final String GEO_SYSTEMFIELD_DELIMITER = ":::";
  public static final String GEO_SYSTEMFIELD_INSERTDOC_COUNTER = "GEO_insert_document_counter";
  public static final String GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_DOCS = "GEO_storage_docs_count_docs";
  public static final String GEO_SYSTEMFIELD_TOTALDOCS_COUNT = "GEO_total_docs_count";
  private static final String GEO_METAFIELD_DOCID = "GEO_doc_id";
  private static final String GEO_METAFIELD_INSERTDOC = "GEO_insert_document";
  private static final String GEO_FIELD_DOC_ID = "_id";
  private static final String GEO_FIELD_DOC_TYPE = "type";
  // TODO: remove unnecessary fields/properties
  private static final String GEO_FIELD_DOC_PROPERTIES = "properties";
  private static final String GEO_FIELD_DOC_PROPERTIES_OBJ_OBJECTID = "OBJECTID";
  private static final String GEO_FIELD_DOC_PROPERTIES_OBJ_INCIDENT_NUMBER = "INCIDENT_NUMBER";
  private static final String GEO_FIELD_DOC_PROPERTIES_OBJ_LOCATION = "LOCATION";
  private static final String GEO_FIELD_DOC_PROPERTIES_OBJ_NOTIFICATION = "NOTIFICATION";
  private static final String GEO_FIELD_DOC_PROPERTIES_OBJ_INCIDENT_DATE = "INCIDENT_DATE";
  private static final String GEO_FIELD_DOC_PROPERTIES_OBJ_TAG_COUNT = "TAG_COUNT";
  private static final String GEO_FIELD_DOC_PROPERTIES_OBJ_MONIKER_CLASS = "MONIKER_CLASS";
  private static final String GEO_FIELD_DOC_PROPERTIES_OBJ_SQ_FT = "SQ_FT";
  private static final String GEO_FIELD_DOC_PROPERTIES_OBJ_PROP_TYPE = "PROP_TYPE";
  private static final String GEO_FIELD_DOC_PROPERTIES_OBJ_WAIVER = "Waiver";
  private static final String GEO_FIELD_DOC_GEOMETRY = "geometry";
  private static final String GEO_FIELD_DOC_GEOMETRY_OBJ_TYPE = "type";
  private static final String GEO_FIELD_DOC_GEOMETRY_OBJ_COORDINATES = "coordinates";
  private final Set<String> allGeoFields = new HashSet<String>() {
    {
      add(GEO_FIELD_DOC_ID);
      add(GEO_FIELD_DOC_TYPE);
      add(GEO_FIELD_DOC_PROPERTIES);
      add(GEO_FIELD_DOC_PROPERTIES_OBJ_OBJECTID);
      add(GEO_FIELD_DOC_PROPERTIES_OBJ_INCIDENT_NUMBER);
      add(GEO_FIELD_DOC_PROPERTIES_OBJ_LOCATION);
      add(GEO_FIELD_DOC_PROPERTIES_OBJ_NOTIFICATION);
      add(GEO_FIELD_DOC_PROPERTIES_OBJ_INCIDENT_DATE);
      add(GEO_FIELD_DOC_PROPERTIES_OBJ_TAG_COUNT);
      add(GEO_FIELD_DOC_PROPERTIES_OBJ_MONIKER_CLASS);
      add(GEO_FIELD_DOC_PROPERTIES_OBJ_SQ_FT);
      add(GEO_FIELD_DOC_PROPERTIES_OBJ_PROP_TYPE);
      add(GEO_FIELD_DOC_PROPERTIES_OBJ_WAIVER);
      add(GEO_FIELD_DOC_GEOMETRY);
      add(GEO_FIELD_DOC_GEOMETRY_OBJ_TYPE);
      add(GEO_FIELD_DOC_GEOMETRY_OBJ_COORDINATES);
    }
  };
  private final int storedDocsCountCustomer = 0;
  private final int storedDocsCountOrder = 0;
  private final Random rand = new Random();
  private final Properties properties;
  /**
   * Author: original author Yuvraj Kanwar, adapted by Danylo Kravchenko
   * <p>
   * The storage-based generator is fetching pre-generated values/documents from an internal in-memory database instead
   * of generating new random values on the fly.
   * This approach allows YCSB to operate with real (or real-looking) JSON documents rather then synthetic.
   * <p>
   * It also provides the ability to query rich JSON documents by splitting JSON documents into query predicates
   * (field, value, type, field-value relation, logical operation)
   */

  private int totalDocsCount = 0;
  private int storedDocsCountDocs = 0;
  private boolean allValuesInitialized = false;
  private int queryLimitMin = 0;
  private int queryLimitMax = 0;
  private int queryOffsetMin = 0;
  private int queryOffsetMax = 0;
  private boolean isZipfian = false;
  private boolean isLatest = false;
  private ZipfianGenerator zipfianGenerator = null;
  private DataFilter geoPredicate;


  public GeoGenerator(Properties p) {
    properties = p;

    queryLimitMin =
        Integer.parseInt(p.getProperty(GeoWorkload.GEO_QUERY_LIMIT_MIN, GeoWorkload.GEO_QUERY_LIMIT_MIN_DEFAULT));
    queryLimitMax =
        Integer.parseInt(p.getProperty(GeoWorkload.GEO_QUERY_LIMIT_MAX, GeoWorkload.GEO_QUERY_LIMIT_MAX_DEFAULT));
    if (queryLimitMax < queryLimitMin) {
      int buff = queryLimitMax;
      queryLimitMax = queryLimitMin;
      queryLimitMin = buff;
    }

    queryOffsetMin =
        Integer.parseInt(p.getProperty(GeoWorkload.GEO_QUERY_OFFSET_MIN, GeoWorkload.GEO_QUERY_OFFSET_MIN_DEFAULT));
    queryOffsetMax =
        Integer.parseInt(p.getProperty(GeoWorkload.GEO_QUERY_OFFSET_MAX, GeoWorkload.GEO_QUERY_OFFSET_MAX_DEFAULT));
    if (queryOffsetMax < queryOffsetMin) {
      int buff = queryOffsetMax;
      queryOffsetMax = queryOffsetMin;
      queryOffsetMin = buff;
    }

    isZipfian = p.getProperty(GeoWorkload.GEO_REQUEST_DISTRIBUTION, GeoWorkload.GEO_REQUEST_DISTRIBUTION_DEFAULT)
        .equals("zipfian");
    isLatest = p.getProperty(GeoWorkload.GEO_REQUEST_DISTRIBUTION, GeoWorkload.GEO_REQUEST_DISTRIBUTION_DEFAULT)
        .equals("latest");
  }


  protected abstract void setVal(String key, String value);


  protected abstract String getVal(String key);


  protected abstract int increment(String key, int step);


  public final Set<String> getAllGeoFields() {
    return allGeoFields;
  }


  public void putDocument(String docKey, String docBody) throws
      Exception {
    HashMap<String, String> tokens = tokenize(docBody);
    String prefix = GEO_DOCUMENT_PREFIX_COLLECTION + GEO_SYSTEMFIELD_DELIMITER;
    int storageCount = increment(prefix + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_DOCS, 1) - 1;

    setVal(prefix + GEO_METAFIELD_DOCID + GEO_SYSTEMFIELD_DELIMITER + storageCount, docKey);
    setVal(prefix + GEO_METAFIELD_INSERTDOC + GEO_SYSTEMFIELD_DELIMITER + storageCount, docBody);

    for (String key : tokens.keySet()) {
      String storageKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + storageCount;
      String value = tokens.get(key);
      if (value != null) {
        setVal(storageKey, value);
      } else {
        for (int i = (storageCount - 1); i > 0; i--) {
          String prevKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + i;
          String prevVal = getVal(prevKey);
          if (prevVal != null) {
            setVal(storageKey, prevVal);
            break;
          }
        }
      }
    }

    //make sure all values are initialized
    if ((!allValuesInitialized) && (storageCount > 1)) {
      boolean nullDetected = false;
      for (String key : tokens.keySet()) {
        for (int i = 0; i < storageCount; i++) {
          String storageKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + i;
          String storageValue = getVal(storageKey);
          if (storageValue != null) {
            for (int j = i; j >= 0; j--) {
              storageKey = prefix + key + GEO_SYSTEMFIELD_DELIMITER + j;
              setVal(storageKey, storageValue);
            }
            break;
          } else {
            nullDetected = true;
          }
        }
      }
      allValuesInitialized = !nullDetected;
    }
  }


  public DataFilter getGeoPredicate() {
    return geoPredicate;
  }


  public void buildGeoReadPredicate() {
    String storageKey = GEO_DOCUMENT_PREFIX_COLLECTION + GEO_SYSTEMFIELD_DELIMITER + GEO_METAFIELD_INSERTDOC +
        GEO_SYSTEMFIELD_DELIMITER + getDocIdWithDistribution();

    String docBody = getVal(storageKey);
    String keyPrefix = GEO_DOCUMENT_PREFIX_COLLECTION + GEO_SYSTEMFIELD_DELIMITER;
    int docCounter = increment(keyPrefix + GEO_SYSTEMFIELD_INSERTDOC_COUNTER, 1);

    geoPredicate = new DataFilter();
    geoPredicate.setDocid(keyPrefix + docCounter);
    geoPredicate.setValue(docBody);
    DataFilter queryPredicate = new DataFilter();
    queryPredicate.setName(GEO_FIELD_DOC_GEOMETRY);
    JSONObject obj = new JSONObject(geoPredicate.getValue());
    JSONObject jobj = (JSONObject) obj.get("geometry");
    queryPredicate.setValueA(jobj);

    buildGeoInsertDocument();
    DataFilter queryPredicate2 = new DataFilter();
    queryPredicate2.setName(GEO_FIELD_DOC_GEOMETRY);
    double[] latLong2 = {
        -111 - rand.nextDouble(),
        33 + rand.nextDouble()
    };
    JSONArray jsonArray2 = new JSONArray(latLong2);
    JSONObject jobj2 = new JSONObject().put("type", "Point");
    jobj2.put("coordinates", jsonArray2);
    queryPredicate2.setValueA(jobj2);

    buildGeoInsertDocument();
    DataFilter queryPredicate3 = new DataFilter();
    queryPredicate3.setName(GEO_FIELD_DOC_GEOMETRY);
    double[] latLong = {
        -111 - rand.nextDouble(),
        33 + rand.nextDouble()
    };
    JSONArray jsonArray = new JSONArray(latLong);
    double[] latLong3 = {
        -111 - rand.nextDouble(),
        33 + rand.nextDouble()
    };
    double[] latLong4 = {
        -111 - rand.nextDouble(),
        33 + rand.nextDouble()
    };
    JSONArray jsonArray3 = new JSONArray(latLong3);
    JSONArray jsonArray4 = new JSONArray(latLong4);
    JSONArray jsonArray5 = new JSONArray();
    JSONArray jsonArray6 = new JSONArray();
    JSONArray jsonArray7 = new JSONArray();
    jsonArray5.put(jsonArray);
    jsonArray5.put(jsonArray2);
    jsonArray6.put(jsonArray3);
    jsonArray6.put(jsonArray4);
    jsonArray7.put(jsonArray5);
    jsonArray7.put(jsonArray6);
    JSONObject jobj3 = new JSONObject().put("type", "MultiLineString");
    jobj3.put("coordinates", jsonArray7);
    queryPredicate3.setValueA(jobj3);

    buildGeoInsertDocument();
    DataFilter queryPredicate4 = new DataFilter();
    queryPredicate4.setName(GEO_FIELD_DOC_GEOMETRY);
    double[] startLatLong = {
        -111 - rand.nextDouble(),
        33 + rand.nextDouble()
    };
    double[] latLong5 = {
        -111 - rand.nextDouble(),
        startLatLong[1]
    };
    double[] latLong6 = {
        -111 - rand.nextDouble(),
        33 + rand.nextDouble()
    };
    double[] latLong7 = {
        startLatLong[0],
        latLong6[1]
    };
    JSONObject polygonObj = new JSONObject().put("type", "Polygon");
    polygonObj.put("coordinates", new JSONArray().put(
        new JSONArray()
            .put(new JSONArray(startLatLong))
            .put(new JSONArray(latLong5))
            .put(new JSONArray(latLong6))
            .put(new JSONArray(latLong7))
            .put(new JSONArray(startLatLong))
    ));
    queryPredicate4.setValueA(polygonObj);
    geoPredicate.setNestedPredicateD(queryPredicate4);
    geoPredicate.setNestedPredicateC(queryPredicate3);
    geoPredicate.setNestedPredicateB(queryPredicate2);
    geoPredicate.setNestedPredicateA(queryPredicate);
  }


  public void buildGeoInsertDocument() {
    String storageKey = GEO_DOCUMENT_PREFIX_COLLECTION + GEO_SYSTEMFIELD_DELIMITER + GEO_METAFIELD_INSERTDOC +
        GEO_SYSTEMFIELD_DELIMITER + getNumberRandom(getStoredDocsCount());

    String docBody = getVal(storageKey);
    String keyPrefix = GEO_DOCUMENT_PREFIX_COLLECTION + GEO_SYSTEMFIELD_DELIMITER;
    int docCounter = increment(keyPrefix + GEO_SYSTEMFIELD_INSERTDOC_COUNTER, 1);

    geoPredicate = new DataFilter();
    geoPredicate.setDocid(keyPrefix + docCounter);
    geoPredicate.setValue(docBody);
  }


  public void buildGeoUpdatePredicate() {
    buildGeoInsertDocument();
    DataFilter queryPredicate = new DataFilter();
    queryPredicate.setName(GEO_FIELD_DOC_GEOMETRY);
    double[] latLong = {
        -111 - rand.nextDouble(),
        33 + rand.nextDouble()
    };
    JSONArray jsonArray = new JSONArray(latLong);
    JSONObject jobj = new JSONObject().put("type", "Point");
    jobj.put("coordinates", jsonArray);
    queryPredicate.setValueA(jobj);
    geoPredicate.setNestedPredicateA(queryPredicate);
  }


  public String getDocIdRandom() {
    return "" + getNumberRandom(getTotalDocsCount());
  }


  public String getDocIdWithDistribution() {
    if (isZipfian) {
      return getNumberZipfianUnifrom(getTotalDocsCount()) + "";
    }
    if (isLatest) {
      return getNumberZipfianLatests(getTotalDocsCount()) + "";
    }
    return getDocIdRandom();
  }


  public int getRandomLimit() {
    if (queryLimitMax == queryLimitMin) {
      return queryLimitMax;
    }
    return rand.nextInt(queryLimitMax - queryLimitMin + 1) + queryLimitMin;
  }


  public int getRandomOffset() {

    if (queryOffsetMax == queryOffsetMin) {
      return queryOffsetMax;
    }
    return rand.nextInt(queryOffsetMax - queryOffsetMin + 1) + queryOffsetMin;
  }


  private HashMap<String, String> tokenize(String jsonString) {
    HashMap<String, String> tokens = new HashMap<String, String>();
    JSONObject obj = new JSONObject(jsonString);

    try {
      tokenizeFields(obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - plain fields");
      ex.printStackTrace();
    }

    try {
      tokenizeObjects(obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - objects");
      ex.printStackTrace();
    }

    return tokens;
  }


  private void tokenizeFields(JSONObject obj, HashMap<String, String> tokens) {

    //string
    ArrayList<String> stringFields = new ArrayList<>(Collections.singletonList(GEO_FIELD_DOC_TYPE));

    for (String field : stringFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, obj.getString(field));
      }
    }
  }


  private void tokenizeObjects(JSONObject obj, HashMap<String, String> tokens) {
    if (obj.has(GEO_FIELD_DOC_ID) && !obj.isNull(GEO_FIELD_DOC_ID)) {
      if (obj.optJSONObject(GEO_FIELD_DOC_ID) != null) {
        tokens.put(GEO_FIELD_DOC_ID, JSONObject.valueToString(obj.getJSONObject(GEO_FIELD_DOC_ID)));
      } else if (obj.optInt(GEO_FIELD_DOC_ID) != 0) {
        tokens.put(GEO_FIELD_DOC_ID, String.valueOf(obj.getInt(GEO_FIELD_DOC_ID)));
      } else {
        tokens.put(GEO_FIELD_DOC_ID, obj.getString(GEO_FIELD_DOC_ID));
      }
    } else {
      tokens.put(GEO_FIELD_DOC_ID, null);
    }
    //1-level nested objects
    String field = GEO_FIELD_DOC_PROPERTIES;

    String l1Prefix = field + GEO_SYSTEMFIELD_DELIMITER;
    tokens.put(l1Prefix + GEO_FIELD_DOC_PROPERTIES_OBJ_OBJECTID, null);
    tokens.put(l1Prefix + GEO_FIELD_DOC_PROPERTIES_OBJ_INCIDENT_NUMBER, null);
    tokens.put(l1Prefix + GEO_FIELD_DOC_PROPERTIES_OBJ_LOCATION, null);
    tokens.put(l1Prefix + GEO_FIELD_DOC_PROPERTIES_OBJ_NOTIFICATION, null);
    tokens.put(l1Prefix + GEO_FIELD_DOC_PROPERTIES_OBJ_INCIDENT_DATE, null);
    tokens.put(l1Prefix + GEO_FIELD_DOC_PROPERTIES_OBJ_TAG_COUNT, null);
    tokens.put(l1Prefix + GEO_FIELD_DOC_PROPERTIES_OBJ_MONIKER_CLASS, null);
    tokens.put(l1Prefix + GEO_FIELD_DOC_PROPERTIES_OBJ_SQ_FT, null);
    tokens.put(l1Prefix + GEO_FIELD_DOC_PROPERTIES_OBJ_PROP_TYPE, null);
    tokens.put(l1Prefix + GEO_FIELD_DOC_PROPERTIES_OBJ_WAIVER, null);

    if (obj.has(field) && !obj.isNull(field)) {
      JSONObject inobj = obj.getJSONObject(field);

      ArrayList<String> inobjStringFields = new ArrayList<>(
          Arrays.asList(GEO_FIELD_DOC_PROPERTIES_OBJ_INCIDENT_NUMBER, GEO_FIELD_DOC_PROPERTIES_OBJ_LOCATION,
              GEO_FIELD_DOC_PROPERTIES_OBJ_NOTIFICATION, GEO_FIELD_DOC_PROPERTIES_OBJ_INCIDENT_DATE,
              GEO_FIELD_DOC_PROPERTIES_OBJ_MONIKER_CLASS, GEO_FIELD_DOC_PROPERTIES_OBJ_PROP_TYPE,
              GEO_FIELD_DOC_PROPERTIES_OBJ_WAIVER));

      for (String infield : inobjStringFields) {
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + GEO_SYSTEMFIELD_DELIMITER + infield;
          tokens.put(key, inobj.getString(infield));
        }
        //integer
        ArrayList<String> intFields = new ArrayList<>(
            Arrays.asList(GEO_FIELD_DOC_PROPERTIES_OBJ_OBJECTID, GEO_FIELD_DOC_PROPERTIES_OBJ_TAG_COUNT,
                GEO_FIELD_DOC_PROPERTIES_OBJ_SQ_FT));

        for (String intfield : intFields) {
          if (inobj.has(intfield) && !inobj.isNull(intfield)) {
            String key = field + GEO_SYSTEMFIELD_DELIMITER + intfield;
            tokens.put(key, String.valueOf(inobj.getInt(intfield)));
          }
        }
      }
    }

    //geospatial objects
    String geoField = GEO_FIELD_DOC_GEOMETRY;

    String lPrefix = geoField + GEO_SYSTEMFIELD_DELIMITER;
    tokens.put(lPrefix + GEO_FIELD_DOC_GEOMETRY_OBJ_TYPE, null);
    tokens.put(lPrefix + GEO_FIELD_DOC_GEOMETRY_OBJ_COORDINATES, null);

    if (obj.has(geoField) && !obj.isNull(geoField)) {
      JSONObject ingobj = obj.getJSONObject(geoField);

      ArrayList<String> ingeoobjStringFields =
          new ArrayList<>(Collections.singletonList(GEO_FIELD_DOC_GEOMETRY_OBJ_TYPE));

      for (String gfield : ingeoobjStringFields) {
        if (ingobj.has(gfield) && !ingobj.isNull(gfield)) {
          String key = geoField + GEO_SYSTEMFIELD_DELIMITER + gfield;
          tokens.put(key, ingobj.getString(gfield));
        }
      }

      String coord = GEO_FIELD_DOC_GEOMETRY_OBJ_COORDINATES;
      JSONArray arr = ingobj.getJSONArray(coord);
      if (arr.length() > 0) {
        String key = geoField + GEO_SYSTEMFIELD_DELIMITER + coord;
        tokens.put(key, arr.getLong(0) + "," + arr.getLong(1));
      }
    }
  }


  private int getStoredDocsCount() {
    if (storedDocsCountDocs == 0) {
      storedDocsCountDocs = Integer.parseInt(getVal(
          GEO_DOCUMENT_PREFIX_COLLECTION + GEO_SYSTEMFIELD_DELIMITER + GEO_SYSTEMFIELD_STORAGEDOCS_COUNT_DOCS));
    }
    return storedDocsCountDocs;
  }


  private int getTotalDocsCount() {
    if (totalDocsCount == 0) {
      totalDocsCount = Integer.parseInt(
          getVal(GEO_DOCUMENT_PREFIX_COLLECTION + GEO_SYSTEMFIELD_DELIMITER + GEO_SYSTEMFIELD_TOTALDOCS_COUNT));
    }
    return totalDocsCount;
  }


  private int getNumberZipfianUnifrom(int totalItems) {
    if (zipfianGenerator == null) {
      zipfianGenerator = new ZipfianGenerator(1L, Long.valueOf(getStoredDocsCount() - 1).longValue());
    }
    return totalItems - zipfianGenerator.nextValue().intValue();
  }


  //getting latest docId shifted back on (max limit + max offest) to ensure the query returns expected amount of results
  private int getNumberZipfianLatests(int totalItems) {
    if (zipfianGenerator == null) {
      zipfianGenerator = new ZipfianGenerator(1L, Long.valueOf(getStoredDocsCount() - 1).longValue());
    }
    return totalItems - zipfianGenerator.nextValue().intValue() - queryLimitMax - queryOffsetMax;
  }


  private int getNumberRandom(int limit) {
    return rand.nextInt(limit);
  }


  /**
   * Created by Yuvraj Singh Kanwar on 2/22/19.
   */
  public class DataFilter {

    public static final String GEO_PREDICATE_TYPE_STRING = "string";
    public static final String GEO_PREDICATE_TYPE_INTEGER = "int";
    public static final String GEO_PREDICATE_TYPE_BOOLEAN = "bool";


    private String name;
    private JSONObject valueA;
    private JSONArray valueB;
    private String value;
    private String docid;
    private Double[] coordinates;
    private Double[] coordinates2;
    private String operation;
    private String relation;
    private String type = GEO_PREDICATE_TYPE_STRING;


    private DataFilter nestedPredicateA;
    private DataFilter nestedPredicateB;
    private DataFilter nestedPredicateC;
    private DataFilter nestedPredicateD;


    public String getName() {
      return name;
    }


    public void setName(String newName) {
      this.name = newName;
    }


    public JSONObject getValueA() {
      return valueA;
    }


    public void setValueA(JSONObject newValueA) {
      this.valueA = newValueA;
    }


    public JSONArray getValueB() {
      return valueB;
    }


    public void setValueB(JSONArray newValueB) {
      this.valueB = newValueB;
    }


    public String getValue() {
      return value;
    }


    public void setValue(String val) {
      this.value = val;
    }


    public String getDocid() {
      return docid;
    }


    public void setDocid(String newDocId) {
      this.docid = newDocId;
    }


    public Double[] getCoordinates() {
      return coordinates;
    }


    public void setCoordinates(Double[] val) {
      this.coordinates = val;
    }


    public Double[] getCoordinates2() {
      return coordinates2;
    }


    public void setCoordinates2(Double[] val) {
      this.coordinates2 = val;
    }


    public String getOperation() {
      return operation;
    }


    public void setOperation(String newOperation) {
      this.operation = newOperation;
    }


    public String getRelation() {
      return relation;
    }


    public void setRelation(String newRelation) {
      this.relation = newRelation;
    }


    public String getType() {
      return type;
    }


    public void setType(String newType) {
      this.type = newType;
    }


    public DataFilter getNestedPredicateA() {
      return nestedPredicateA;
    }


    public void setNestedPredicateA(DataFilter newNestedPredicateA) {
      this.nestedPredicateA = newNestedPredicateA;
    }


    public DataFilter getNestedPredicateB() {
      return nestedPredicateB;
    }


    public void setNestedPredicateB(DataFilter newNestedPredicateB) {
      this.nestedPredicateB = newNestedPredicateB;
    }


    public DataFilter getNestedPredicateC() {
      return nestedPredicateC;
    }


    public void setNestedPredicateC(DataFilter newNestedPredicateC) {
      this.nestedPredicateC = newNestedPredicateC;
    }


    public DataFilter getNestedPredicateD() {
      return nestedPredicateD;
    }


    public void setNestedPredicateD(DataFilter newNestedPredicateD) {
      this.nestedPredicateD = newNestedPredicateD;
    }

  }

}
