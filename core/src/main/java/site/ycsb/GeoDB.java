package site.ycsb;

import site.ycsb.generator.GeoGenerator;

import java.util.HashMap;
import java.util.Vector;

/**
 * Author: original Yuvraj Kanwar, adjusted by Danylo Kravchenko
 * A layer for accessing a database to be benchmarked. Each thread in the client
 * will be given its own instance of whatever DB class is to be used in the test.
 * This class should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 *
 * Note that YCSB does not make any use of the return codes returned by this class.
 * Instead, it keeps a count of the return values and presents them to the user.
 *
 * The semantics of methods such as insert, update and delete vary from database
 * to database.  In particular, operations may or may not be durable once these
 * methods commit, and some systems may return 'success' regardless of whetherF
 * or not a tuple with a matching key existed before the call.  Rather than dictate
 * the exact semantics of these methods, we recommend you either implement them
 * to match the database's default semantics, or the semantics of your
 * target application.  For the sake of comparison between experiments we also
 * recommend you explain the semantics you chose when presenting performance results.
 */
public abstract class GeoDB extends DB {

  /**
   *
   *  GEO operations.
   *
   */

  // overloading the standard "insert" operation as it used by YCSB for loading data
  public Status geoLoad(String table, GeoGenerator generator, Double recordCount) {
    return null;
  }

  public Status geoInsert(String table, HashMap<String, ByteIterator> result, GeoGenerator gen)  {
    System.err.println("geoInsert not implemented");
    return null;
  }

  public Status geoUpdate(String table, HashMap<String, ByteIterator> result, GeoGenerator gen)  {
    System.err.println("geoUpdate not implemented");
    return null;
  }

  public Status geoNear(String table, HashMap<String, ByteIterator> result, GeoGenerator gen)  {
    System.err.println("geoNear not implemented");
    return null;
  }

  public Status geoBox(String table, HashMap<String, ByteIterator> result, GeoGenerator gen)  {
    System.err.println("geoBox not implemented");
    return null;
  }

  public Status geoIntersect(String table, HashMap<String, ByteIterator> result, GeoGenerator gen)  {
    System.err.println("geoIntersect not implemented");
    return null;
  }

  public Status geoScan(String table, Vector<HashMap<String, ByteIterator>> result, GeoGenerator gen)  {
    System.err.println("geoScan not implemented");
    return null;
  }

}