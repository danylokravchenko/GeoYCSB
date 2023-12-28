package site.ycsb.workloads;

import site.ycsb.ByteIterator;
import site.ycsb.GeoDB;
import site.ycsb.Status;
import site.ycsb.generator.DiscreteGenerator;
import site.ycsb.generator.GeoGenerator;
import site.ycsb.generator.MemcachedGenerator;
import site.ycsb.WorkloadException;

import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;

/**
 * Author: original Yuvraj Kanwar. Adapted by Danylo Kravchenko
 */
public class GeoWorkload extends CoreWorkload {

  protected DiscreteGenerator operationchooser;


  public static final String STORAGE_HOST = "geo_storage_host";
  public static final String STORAGE_HOST_DEFAULT = "localhost";
  public static final String STORAGE_PORT = "geo_storage_port";
  public static final String STORAGE_PORT_DEFAULT = "11211";
  public static final String TOTAL_DOCS = "totalrecordcount";
  public static final String TOTAL_DOCS_DEFAULT = "13348";
  public static final String DOCS_START_VALUE = "1001";

  public static final String RECORD_COUNT = "recordcount";
  public static final String RECORD_COUNT_DEFAULT = "1000000";

  public static final String GEO_INSERT_PROPORTION_PROPERTY = "geo_insert";
  public static final String GEO_INSERT_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String GEO_UPDATE_PROPORTION_PROPERTY = "geo_update";
  public static final String GEO_UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String GEO_NEAR_PROPORTION_PROPERTY = "geo_near";
  public static final String GEO_NEAR_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String GEO_BOX_PROPORTION_PROPERTY = "geo_box";
  public static final String GEO_BOX_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String GEO_INTERSECT_PROPORTION_PROPERTY = "geo_intersect";
  public static final String GEO_INTERSECT_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String GEO_SCAN_PROPORTION_PROPERTY = "geo_scan";
  public static final String GEO_SCAN_PROPORTION_PROPERTY_DEFAULT = "0.00";


  public static final String GEO_QUERY_LIMIT_MIN = "geo_querylimit_min";
  public static final String GEO_QUERY_LIMIT_MIN_DEFAULT = "10";
  public static final String GEO_QUERY_LIMIT_MAX = "geo_querylimit_max";
  public static final String GEO_QUERY_LIMIT_MAX_DEFAULT = "100";

  public static final String GEO_QUERY_OFFSET_MIN = "geo_offset_min";
  public static final String GEO_QUERY_OFFSET_MIN_DEFAULT = "10";
  public static final String GEO_QUERY_OFFSET_MAX = "geo_offset_max";
  public static final String GEO_QUERY_OFFSET_MAX_DEFAULT = "100";
  public static final String GEO_REQUEST_DISTRIBUTION = "geo_request_distribution";
  public static final String GEO_REQUEST_DISTRIBUTION_DEFAULT = "uniform";
  private static double recordCount = 1000000;

  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    String memHost = p.getProperty(STORAGE_HOST, STORAGE_HOST_DEFAULT);
    String memPort = p.getProperty(STORAGE_PORT, STORAGE_PORT_DEFAULT);
    String totalDocs = p.getProperty(TOTAL_DOCS, TOTAL_DOCS_DEFAULT);
    try {
      return new MemcachedGenerator(p, memHost, memPort, totalDocs);
    } catch (Exception e) {
      System.err.println("Memcached generator init failed " + e.getMessage());
      throw new WorkloadException();
    }
  }


  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    operationchooser = createOperationGenerator(p);
    recordCount = Double.parseDouble(
        p.getProperty(RECORD_COUNT, RECORD_COUNT_DEFAULT));
  }

  @Override
  public boolean doInsert(GeoDB db, Object threadstate) {
    Status status;
    status = db.geoLoad(table, (MemcachedGenerator) threadstate, recordCount);
    return null != status && status.isOk();
  }



  @Override
  public boolean doTransaction(GeoDB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }
    MemcachedGenerator  generator = (MemcachedGenerator) threadstate;
    System.out.println(operation);
    switch (operation) {
      case "READ":
        doTransactionRead(db);
        break;
      case "UPDATE":
        doTransactionUpdate(db);
        break;
      case "INSERT":
        doTransactionInsert(db);
        break;
      case "GEO_INSERT":
        doTransactionGeoInsert(db, generator);
        break;
      case "GEO_UPDATE":
        doTransactionGeoUpdate(db, generator);
        break;
      case "GEO_NEAR":
        doTransactionGeoNear(db, generator);
        break;
      case "GEO_BOX":
        doTransactionGeoBox(db, generator);
        break;
      case "GEO_INTERSECT":
        doTransactionGeoIntersect(db, generator);
        break;
      case "GEO_SCAN":
        doTransactionGeoScan(db, generator);
        break;
      default:
        doTransactionReadModifyWrite(db);
    }

    return true;
  }


  public void doTransactionGeoInsert(GeoDB db, GeoGenerator generator) {
    try {
      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
      db.geoInsert(table, cells, generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionGeoUpdate(GeoDB db, GeoGenerator generator) {
    try {
      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
      db.geoUpdate(table, cells, generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionGeoNear(GeoDB db, GeoGenerator generator) {
    try {
      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
      db.geoNear(table, cells, generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionGeoBox(GeoDB db, GeoGenerator generator) {
    try {
      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
      db.geoBox(table, cells, generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionGeoIntersect(GeoDB db, GeoGenerator generator) {
    try {
      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
      db.geoIntersect(table, cells, generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionGeoScan(GeoDB db, GeoGenerator generator) {
    try {
      db.geoScan(table, new Vector<HashMap<String, ByteIterator>>(), generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }


  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(
        p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    final double updateproportion = Double.parseDouble(
        p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double insertproportion = Double.parseDouble(
        p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double scanproportion = Double.parseDouble(
        p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
        READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));


    final double geoInsert = Double.parseDouble(
        p.getProperty(GEO_INSERT_PROPORTION_PROPERTY, GEO_INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double geoUpdate = Double.parseDouble(
        p.getProperty(GEO_UPDATE_PROPORTION_PROPERTY, GEO_UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double geoNear = Double.parseDouble(
        p.getProperty(GEO_NEAR_PROPORTION_PROPERTY, GEO_NEAR_PROPORTION_PROPERTY_DEFAULT));
    final double geoBox = Double.parseDouble(
        p.getProperty(GEO_BOX_PROPORTION_PROPERTY, GEO_BOX_PROPORTION_PROPERTY_DEFAULT));
    final double geoIntersect = Double.parseDouble(
        p.getProperty(GEO_INTERSECT_PROPORTION_PROPERTY, GEO_INTERSECT_PROPORTION_PROPERTY_DEFAULT));
    final double geoScan = Double.parseDouble(
        p.getProperty(GEO_SCAN_PROPORTION_PROPERTY, GEO_SCAN_PROPORTION_PROPERTY_DEFAULT));

    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }

    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
    }

    if (insertproportion > 0) {
      operationchooser.addValue(insertproportion, "INSERT");
    }

    if (scanproportion > 0) {
      operationchooser.addValue(scanproportion, "SCAN");
    }

    if (readmodifywriteproportion > 0) {
      operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
    }

    if (geoInsert > 0) {
      operationchooser.addValue(geoInsert, "GEO_INSERT");
    }

    if (geoUpdate > 0) {
      operationchooser.addValue(geoUpdate, "GEO_UPDATE");
    }

    if (geoNear > 0) {
      operationchooser.addValue(geoNear, "GEO_NEAR");
    }

    if (geoBox > 0) {
      operationchooser.addValue(geoBox, "GEO_BOX");
    }

    if (geoIntersect > 0) {
      operationchooser.addValue(geoIntersect, "GEO_INTERSECT");
    }

    if (geoScan > 0) {
      operationchooser.addValue(geoScan, "GEO_SCAN");
    }

    return operationchooser;
  }
}

