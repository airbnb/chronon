# Stripe Chronon Setup Instructions

_These docs are very WIP. Please don't hesitate to make PRs with changes
to the instructions, or to get in touch with us in #ml-features-shepherd on Slack!_

## Step 1: Install Docker
You can install from Managed Software Center. 

## Step 2: Build a local image from `docker/local/Dockerfile`

We've had issues getting the project to build on Stripe laptops, likely due to incompatible dependencies. In this step, you'll create a Docker container with all the required dependencies for building the project and running tests.

In the `chronon/` directory, run `docker build --progress=plain -f docker/local/Dockerfile . -t chronon_local:latest` This should take about 10-15 minutes on a M1 Max Macbook Pro.

Then, run `docker images`, and you should see a new image:

```shell
st-andrewlee2:docker andrewlee$ docker images
REPOSITORY                                                                        TAG            IMAGE ID       CREATED          SIZE
chronon_local                                                                     latest         a0c28e2a0fc7   23 minutes ago   1.84GB
...
```

(In the future we'll get a prebuilt container image into [Stripe's container registry](https://amp.corp.stripe.com/containers/northwest)
so folks won't have to go through this step)

## Step 3: Building and testing!

To get a shell for your container, run `docker run -v ~/stripe/chronon/:/chronon --rm -it chronon_local`

### Compiling and testing Scala

On your container shell, run `sbt "++ 2.12.12 compile"`. (Scala 2.12.12 is preinstalled on the container, without it `sbt` will try to 
download a different Scala version) 

Compiling took about a minute on an M1 Max Macbook Pro.

Run `sbt "++ 2.12.12 test"`. This took about 10-15 minutes on an M1 Max Macbook Pro, the lion's share of which is spent running Spark tests. We recommend running all the tests once to ensure all dependencies are properly configured. During development, you can specify a particular test to run in `sbt` e.g. `sbt "testOnly *GroupByTest"`, as instructed here: https://git.corp.stripe.com/stripe-private-oss-forks/chronon/blob/master/devnotes.md#L63

### Running tests on IntelliJ
Running `compile` in the container will generate Thrift classes on your laptop at `api/thrift/api.thrift` into folder `/chronon/api/target/scala-2.12/src_managed/main` and IntelliJ will be able to pick them up. IntelliJ should allow you to run tests inline (look for the green triangles next to test classes and methods!)

### Setting up and testing Python

Run the following setup script, which will generate Python code for thrifts and 
install Python packages for tests:
```
bash /py_container_setup.sh  
```

To run the Python tests, run `tox` from `/chronon/api/py`.
