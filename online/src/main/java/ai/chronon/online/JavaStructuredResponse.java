/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online;

public class JavaStructuredResponse {
    public JavaRequest request;
    public JTry<JavaFeatureRecord> values;

    public JavaStructuredResponse(JavaRequest request, JTry<JavaFeatureRecord> values) {
        this.request = request;
        this.values = values;
    }

    public JavaStructuredResponse(Fetcher.StructuredResponse scalaResponse) {
        this.request = new JavaRequest(scalaResponse.request());
        this.values = JTry
                .fromScala(scalaResponse.values())
                .map(v -> v == null ? null : new JavaFeatureRecord(v));
    }
}
