package org.dartov.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Returns the longest prefix of a number found in lookup table
 *
 * @author dartov
 */
@Description(name = "longest_prefix_str", value = "_FUNC_(s, lookupfile) – Returns the longest prefix of a number found in lookup table")
public class LookupPrefixString extends GenericUDF {

    private StringObjectInspector numberInspector;
    private StringObjectInspector fileInspector;

    // This lookup contains pairs (prefix, prefix_cd)
    private Map<String, String> lookup;


    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentLengthException("This functions needs two arguments: number string column, and source file for lookup");
        }
        this.numberInspector = (StringObjectInspector) objectInspectors[0];
        this.fileInspector = (StringObjectInspector) objectInspectors[1];
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Text evaluate(DeferredObject[] deferredObjects) throws HiveException {
       if (lookup == null) {
            initHdfsLookup(fileInspector.getPrimitiveJavaObject(deferredObjects[1].get()));
        }
        String searchPrefix = (String) numberInspector.getPrimitiveJavaObject(deferredObjects[0].get());

        while (!searchPrefix.isEmpty()) {
            if (lookup.containsKey(searchPrefix)) {
                return new Text(lookup.get(searchPrefix));
            } else {
                searchPrefix = reducePrefixByOne(searchPrefix);
            }
        }
        return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "Method call: longest_prefix_str(" + strings[0] + ", " + strings[1] + ")";
    }

    private void initHdfsLookup(String lookupFile) throws HiveException {
        try {
            Configuration conf = new Configuration();
            Path filePath = new Path(lookupFile);

            FileSystem fs = FileSystem.get(filePath.toUri(), conf);
            FSDataInputStream in = fs.open(filePath);

            initMap(in);
        } catch (Exception e) {
            throw new HiveException(e + ": when attempting to access: " + lookupFile);
        }
    }

    protected void initMap(InputStream in) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        lookup = new HashMap<String, String>();
        while ((line = reader.readLine()) != null) {
            String[] split = line.split(",");
            lookup.put(split[0], split[1]);
        }
        reader.close();
    }

    protected String reducePrefixByOne(String largerPrefix) {
        if (largerPrefix != null && largerPrefix.length() > 0) {
            return largerPrefix.substring(0, largerPrefix.length() - 1);
        } else {
            return largerPrefix;
        }
    }
}
