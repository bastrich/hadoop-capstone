package com.bastrich;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

import java.util.List;

public class IsIpOfSubnets extends GenericUDF {

    private PrimitiveObjectInspector ipOI;
    private ListObjectInspector subnetOI;
    private PrimitiveObjectInspector resultOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        assert (objectInspectors.length == 2);
        assert(objectInspectors[0].getCategory() == ObjectInspector.Category.PRIMITIVE);
        assert(objectInspectors[1].getCategory() == ObjectInspector.Category.LIST);

        ipOI  = (PrimitiveObjectInspector)objectInspectors[0];
        subnetOI  = (ListObjectInspector)objectInspectors[1];

        assert(ipOI.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING);
        assert(subnetOI.getListElementObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE);

        resultOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
        return resultOI;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects.length != 2) return null;

        Object ipObject = deferredObjects[0].get();
        if (ipObject == null) return null;
        Object subnetObject = deferredObjects[1].get();
        if (subnetObject == null) return null;

        String ip = (String) ipOI.getPrimitiveJavaObject(ipObject);
        List<String> subnets = (List<String>) subnetOI.getList(subnetObject);

        boolean result = false;
        for (String subnetString: subnets) {
            SubnetUtils.SubnetInfo subnet = (new SubnetUtils(subnetString)).getInfo();
            if (subnet.isInRange(ip)) {
                result = true;
                break;
            }
        }

        return new BooleanWritable(result);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "Checks if IP (the first arguments) relates to at least one of SUBNETS (the second argument)";
    }

    @Override
    public String getFuncName() {
        return "is_ip_of_subnet";
    }
}