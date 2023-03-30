package ai.chronon.online;

import scala.Option;
import scala.util.ScalaVersionSpecificCollectionsConverter;

import java.util.Map;

public class JavaStatsRequest {
  public String name;
  public Long startTs;
  public Long endTs;

  public JavaStatsRequest(String name) {
    this(name, null, null);
  }
  public JavaStatsRequest(String name, Long startTs) {
    this.name = name;
    this.startTs = startTs;
    this.endTs =  null;
  }
  public JavaStatsRequest(String name, Long startTs, Long endTs) {
    this.name = name;
    this.startTs = startTs;
    this.endTs = endTs;
  }

  public JavaStatsRequest(Fetcher.StatsRequest scalaRequest) {
    this.name = scalaRequest.name();
    Option<Object> startTsOpt = scalaRequest.startTs();
    Option<Object> endTsOpt = scalaRequest.endTs();
    if (startTsOpt.isDefined()) {
      this.startTs = (Long) startTsOpt.get();
    }
    if (endTsOpt.isDefined()) {
      this.endTs = (Long) endTsOpt.get();
    }
  }

  public static JavaStatsRequest fromScalaRequest(Fetcher.StatsRequest scalaRequest) {
    return new JavaStatsRequest(scalaRequest);
  }

  public Fetcher.StatsRequest toScalaRequest() {
    return new Fetcher.StatsRequest(
        this.name,
        Option.apply(this.startTs),
        Option.apply(this.endTs));
  }
}


