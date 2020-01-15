///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// * <p>
// * http://www.apache.org/licenses/LICENSE-2.0
// * <p>
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.bastrich;
//
//import inet.ipaddr.IPAddress;
//import inet.ipaddr.IPAddressString;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hive.ql.exec.Description;
//import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
//import org.apache.hadoop.hive.ql.metadata.HiveException;
//import org.apache.hadoop.hive.ql.parse.SemanticException;
//import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
//import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
//import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
//import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
//import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
//import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
//import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.stream.Collectors;
//
//@Description(name = "merge_subnets", value = "_FUNC_(expr) - Merge array of subnets to array of subnets")
//public class MergeSubnets implements GenericUDAFResolver2 {
//
//    static final Log LOG = LogFactory.getLog(MergeSubnets.class.getName());
//
//    @Override
//    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
//        if (parameters.length != 1) {
//            throw new UDFArgumentTypeException(parameters.length - 1,
//                    "Please specify only subnet column.");
//        }
//
//        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
//            throw new UDFArgumentTypeException(0,
//                    "Only primitive type arguments are accepted but "
//                            + parameters[0].getTypeName() + " was passed as parameter.");
//        }
//        if (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()
//                != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
//            throw new UDFArgumentTypeException(0,
//                    "Only a String argument is accepted as parameter, but "
//                            + parameters[0].getTypeName() + " was passed instead.");
//        }
//
//        return new MergeSubnetsEvaluator();
//    }
//
//    @Override
//    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
//        return getEvaluator(info.getParameters());
//    }
//
//    public static class MergeSubnetsEvaluator extends GenericUDAFEvaluator {
//
//        private PrimitiveObjectInspector inputOI;
//        private ListObjectInspector resultOI;
//
//        @Override
//        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
//            super.init(m, parameters);
//
//            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
//                assert (parameters.length == 1);
//                inputOI = (PrimitiveObjectInspector) parameters[0];
//            } else {
//                resultOI = (ListObjectInspector) parameters[0];
//            }
//
//            return ObjectInspectorFactory.getStandardListObjectInspector(
//                    PrimitiveObjectInspectorFactory.writableStringObjectInspector
//            );
//        }
//
//        @Override
//        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
//            return terminate(agg);
//        }
//
//        @Override
//        public Object terminate(AggregationBuffer agg) throws HiveException {
//            return ((SubnetAggBuffer) agg).subnets;
//        }
//
//        @Override
//        @SuppressWarnings("unchecked")
//        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
//            List<String> subnets = (List<String>) resultOI.getList(partial);
//            ((SubnetAggBuffer) agg).subnets.addAll(subnets);
//            ((SubnetAggBuffer) agg).subnets = mergeSubnets(((SubnetAggBuffer) agg).subnets);
//        }
//
//        @Override
//        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
//            String subnet = (String) inputOI.getPrimitiveJavaObject(parameters[0]);
//            ((SubnetAggBuffer) agg).subnets.add(subnet);
//            ((SubnetAggBuffer) agg).subnets = mergeSubnets(((SubnetAggBuffer) agg).subnets);
//        }
//
//        static class SubnetAggBuffer extends AbstractAggregationBuffer {
//            List<String> subnets = new LinkedList<>();
//        }
//
//        @Override
//        public SubnetAggBuffer getNewAggregationBuffer() throws HiveException {
//            return new SubnetAggBuffer();
//        }
//
//        @Override
//        public void reset(AggregationBuffer agg) throws HiveException {
//            ((SubnetAggBuffer) agg).subnets = new ArrayList<>();
//        }
//
//        private List<String> mergeSubnets(List<String> subnets) {
//            IPAddress first = new IPAddressString(subnets.get(0)).getAddress();
//            IPAddress[] others = subnets.subList(1, subnets.size()).stream().map(str -> new IPAddressString(str).getAddress()).toArray(IPAddress[]::new);
//            return Arrays.stream(first.mergeToPrefixBlocks(others)).map(IPAddress::toString).collect(Collectors.toCollection(LinkedList::new));
//        }
//    }
//}