/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.fn.hive;

import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.google.common.collect.Sets;
import org.apache.arrow.gandiva.evaluator.ExpressionRegistry;
import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.dremio.exec.expr.fn.HiveFunctionRegistry;
import com.dremio.test.DremioTest;
import com.google.common.collect.ArrayListMultimap;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.junit.Test;

import java.util.Map;
import java.util.Set;


public class TestHiveFunctionRegistry extends DremioTest {
  private static int totalFuncs = 0, unSupportedFn = 0;

  @Test
  public void printGandivaUnsupportedHiveRegistryFunctions() throws GandivaException {
    Set<String> fns = Sets.newHashSet();

    // Retrieve methods from Gandiva function registry and register them on a Set
    Set<FunctionSignature> supportedFunctions = ExpressionRegistry.getInstance()
      .getSupportedFunctions();
    for (FunctionSignature signature : supportedFunctions ) {
      StringBuilder fnName = new StringBuilder((signature.getName().toLowerCase() + "##"));
//      for (ArrowType param : signature.getParamTypes()) {
//        fnName.append("##").append(param.toString());
//      }
      fns.add(fnName.toString());
    }

    // Retrieve methods on Hive Function Registry
    HiveFunctionRegistry registry = new HiveFunctionRegistry(CLASSPATH_SCAN_RESULT);
    ArrayListMultimap<String, Class<? extends UDF>> methodsUDF = registry.getMethodsUDF();
    ArrayListMultimap<String, Class<? extends GenericUDF>> methodsGenericUDF = registry.getMethodsGenericUDF();


    for (Map.Entry<String, Class<? extends UDF>> holders : methodsUDF.entries()) {
      String name = holders.getKey();
      Class<? extends UDF> holder = holders.getValue();
      totalFuncs++;
      isFunctionSupported(name, fns);

    }
    System.out.println("Total : " + totalFuncs + " unSupported : " + unSupportedFn);
  }

  private boolean isFunctionSupported(String name, Set<String> fns) {
    String fnToSearch = FunctionCallFactory.replaceOpWithFuncName(name) + "##";

    if (!fns.contains(fnToSearch)) {
      unSupportedFn++;
      System.out.println(("function signature not supported in gandiva : " +  fnToSearch));
      return false;
    }
//    else {
//      System.out.println(("function signature supported in gandiva : " +  fnToSearch));
//    }
    return true;
  }
}
