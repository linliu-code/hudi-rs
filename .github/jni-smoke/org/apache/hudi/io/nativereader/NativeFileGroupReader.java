/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.nativereader;

/** JNI load smoke for the CI-built libhudi_jni.so: same package/class as the real reader, only the liveness probe. */
public final class NativeFileGroupReader {
  static native String version();

  public static void main(String[] args) {
    System.load(args[0]);
    String v = version();
    System.out.println("SMOKE " + v);
    if (!v.startsWith("hudi-jni ") || !v.contains(" abi=3")) {
      System.exit(1);
    }
  }
}
