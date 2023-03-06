from run import download_jar

def run_sql_gen(results, ds, sampling, chronon_jar, version, release_tag):
    jar_path = chronon_jar if chronon_jar else download_jar(
        version, jar_type="uber",
        release_tag=release_tag,
        spark_version='2.4.0')

    for name, obj in results.items():
        print(f"Generating query for {name}: \n\n")

    args = '--conf-path={conf_path} --end-date={ds} '

    print("HERE!")