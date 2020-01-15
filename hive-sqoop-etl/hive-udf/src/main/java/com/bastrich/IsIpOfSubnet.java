package com.bastrich;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.jboss.netty.handler.ipfilter.IpSubnet;

import java.net.UnknownHostException;

public class IsIpOfSubnet extends GenericUDF {

    private PrimitiveObjectInspector ipOI;
    private PrimitiveObjectInspector subnetOI;
    private PrimitiveObjectInspector resultOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        assert (objectInspectors.length == 2);
        assert(objectInspectors[0].getCategory() == ObjectInspector.Category.PRIMITIVE);
        assert(objectInspectors[1].getCategory() == ObjectInspector.Category.PRIMITIVE);

        ipOI  = (PrimitiveObjectInspector)objectInspectors[0];
        subnetOI  = (PrimitiveObjectInspector)objectInspectors[1];

        assert(ipOI.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING);
        assert(subnetOI.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING);

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
        String subnet = (String) subnetOI.getPrimitiveJavaObject(subnetObject);
        try {
            return new BooleanWritable(new IpSubnet(subnet).contains(ip));
        } catch (UnknownHostException e) {
            throw  new HiveException(e);
        }
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