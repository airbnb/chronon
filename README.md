# Publishing project fat JAR to Artifactory

0. Create MVN settings file under `mvn_settings.xml`.

``` xml

<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                          https://maven.apache.org/xsd/settings-1.0.0.xsd">
    <servers>
        <server>
            <id>airbnb</id>
            <username>ARTIFACTORY_USERNAME</username>
            <password>ARTIFACTORY_PASSWORD</password>
        </server>
    </servers>
</settings>
```
Replace `ARTIFACTORY_USERNAME` and `ARTIFACTORY_PASSWORD` with your username and API key from [Artifactory](https://artifactory.d.musta.ch/artifactory/webapp/#/profile).

1. After you merge your PR, check out and pull `master` branch.
2. Create a tag named `release-zl-X.X.X`. Check `git tag` to find out the next version.

``` shell
git tag -a -m '<tag message>' release-zl-X.X.X
```

3. Publish to artifactory

``` shell
./push_to_artifactory.sh
```
