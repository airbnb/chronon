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

import scala.util.ScalaVersionSpecificCollectionsConverter;

import java.util.Map;


public class JavaStatsResponse {
    public JavaStatsRequest request;
    public JTry<Map<String, Object>> values;
    public Long millis;

    public JavaStatsResponse(JavaStatsRequest request, JTry<Map<String, Object>> values) {
        this.request = request;
        this.values = values;
        this.millis = null;
    }

    public JavaStatsResponse(JavaStatsRequest request, JTry<Map<String, Object>> values, Long millis) {
        this.request = request;
        this.values = values;
        this.millis = millis;
    }

    public JavaStatsResponse(Fetcher.StatsResponse scalaResponse){
        this.request = new JavaStatsRequest(scalaResponse.request());
        this.values = JTry
                .fromScala(scalaResponse.values())
                .map(ScalaVersionSpecificCollectionsConverter::convertScalaMapToJava);
        this.millis = scalaResponse.millis();
    }

}
