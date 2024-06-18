# Contributor Guide

Everyone is welcome to contribute to Chronon. We value all forms of contributions, including, but not limited to:

- Documentation and usage examples
- Community participation in forums and issues.
- Code readability and developer guide
- Logging improvements
- Code comment improvements
- Documentation improvements
- Test cases to make the codebase more robust
- Tutorials, blog posts, talks that promote the project.
- Functionality extensions, new features, etc.
- Optimizations
- Support for new aggregations and data types
- Support for connectors to different storage systems and event buses

In the interest of keeping Chronon a stable platform for users, some changes are discouraged and would be very unlikely to be allowed in. These include, but are not limited to:

- Backwards incompatible API changes, for example adding a required argument without a default to the run.py module or the fetcher library, etc.
- Changes to the aggregation library or spark library that produce different data outputs (such changes would be caught by unit tests and fail to pass).
- Changes that could break online fetching flows, including changing the timestamp watermarking or processing in the lambda architecture, or Serde logic.
- Changes that would interfere with existing Airflow DAGs, for example changing the default schedule in a way that would cause breakage on recent versions of Airflow.

There are exceptions to these general rules, however, please be sure to follow the “major change” guidelines if you wish to make such a change.

## General Development Process

Everyone in the community is welcome to send patches, documents, and propose new features to the project.

Code changes require a stamp of approval from Chronon contributors to be merged, as outlined in the project bylaws.

Larger changes, as well as proposed directions for the project should follow the Chronon Improvement Proposal guide, outlined below.

The process for reporting bugs and requesting smaller features is also outlined below.

## Pull Request Guidelines

Pull Requests (PRs) should follow these guidelines as much as possible:

### Code Guidelines

- Follow our [code style guidelines](docs/source/Code_Guidelines.md)
- Well scoped (avoid multiple unrelated changes in the same PR)
- Code should be rebased on the latest version of the latest version of the master branch
- All lint checks and test cases should pass
- If the change is a bugfix to the aggregations, spark, streaming or fetching libraries,  then a test case that catches the bug should be included
- Similarly, if the PR expands the functionality of these libraries, then test cases should be included to cover new functionality
- Documentation should be added for new code

### Commit Message Guidelines

Chronon uses the Github (GH) platform for patch submission and code review via Pull Requests (PRs). The final commit (title and body) that is merged into the master branch is composed of the PR’s title and body and must be kept updated and reflecting the new changes in the code as per the reviews and discussions.

Although these guidelines apply essentially to the PRs’ title and body messages, because GH auto-generates the PR’s title and body from the commits on a given branch, it’s recommended to follow these guidelines right from the beginning, when preparing commits in general to be submitted to the Chronon project. This will ease the creation of a new PR, avoiding rework, and also will help the review.

The rules below will help to achieve uniformity that has several benefits, both for review and for the code base maintenance as a whole, helping you to write commit messages with a good quality suitable for the Chronon project, allowing fast log searches, bisecting, and so on.

#### PR title

- Guarantee a title exists
- Don’t use Github usernames in the title, like @username (enforced)
- Include tags as a hint about what component(s) of the code the PRs / commits “touch”. For example [BugFix], [CI], [Streaming], [Spark], etc. If more than one tag exist, multiple brackets should be used, like [BugFix][CI]

#### PR body

- Guarantee a body exists
- Include a simple and clear explanation of the purpose of the change
- Include any relevant information about how it was tested

## Release Guidelines

Releases are managed by project committers, as outlined in the project Bylaws.
The committer(s) who approve and help merge a change should be the ones to drive the release process for that change, unless explicitly delegated to someone else.
Please see the release instructions in the code repository.

## Bug Reports

Issues need to contain all relevant information based on the type of the issue. We have four issue types

### Incorrect Outputs

- Summary of what the user was trying to achieve
  - Sample data - Inputs, Expected Outputs (by the user) and Current Output
  - Configuration - StagingQuery / GroupBy or Join
- Repro steps
  - What commands were run and what was the full output of the command
- PR guidelines
  - Includes a failing test case based on sample data

### Crash Reports

- Summary of what the user was trying to achieve
  - Sample data - Inputs, Expected Outputs (by the user)
  - Configuration - StagingQuery / GroupBy or Join
- Repro steps
  - What commands were run and the output along with the error stack trace
- PR guidelines
  - Includes a test case for the crash

## Feature requests and Optimization Requests

We expect the proposer to create a CHIP / Chronon Improvement Proposal document as detailed below

# Chronon Improvement Proposal (CHIP)

## Purpose

The purpose of CHIPs is to have a central place to collect and document planned major enhancements to Chronon. While Github is still the tool to track tasks, bugs, and progress, the CHIPs give an accessible high-level overview of the result of design discussions and proposals. Think of CHIPs as collections of major design documents for relevant changes.
This way of maintaining CHIPs is heavily influenced by the Apache Flink project’s Improvement Proposal guidelines. But instead of doing this through JIRA, we use Github PRs and issues.
We want to make Chronon a core architectural component for users. We also support a large number of integrations with other tools, systems, and clients. Keeping this kind of usage healthy requires a high level of compatibility between releases — core architectural elements can't break compatibility or shift functionality from release to release. As a result each new major feature or public API has to be done in a future proof way.
This means when making this kind of change we need to think through what we are doing as best we can prior to release. And as we go forward we need to stick to our decisions as much as possible. All technical decisions have pros and cons so it is important we capture the thought process that leads to a decision or design to avoid flip-flopping needlessly.

**CHIPs should be proportional in effort to their magnitude — small changes should just need a couple brief paragraphs, whereas large changes need detailed design discussions.**

This process also isn't meant to discourage incompatible changes — proposing an incompatible change is totally legitimate. Sometimes we will have made a mistake and the best path forward is a clean break that cleans things up and gives us a good foundation going forward. Rather this is intended to avoid accidentally introducing half thought-out interfaces and protocols that cause needless heartburn when changed. Likewise the definition of "compatible" is itself squishy: small details like which errors are thrown when are clearly part of the contract but may need to change in some circumstances, likewise performance isn't part of the public contract but dramatic changes may break use cases. So we just need to use good judgment about how big the impact of an incompatibility will be and how big the payoff is.

## What is considered a "major change" that needs a CHIP?

Any of the following should be considered a major change:

- Any major new feature, subsystem, or piece of functionality
- Any change that impacts the public interfaces of the project

All of the following are public interfaces that people build around:

- User facing Python APIs
  - StagingQuery - freeform ETL primitive
  - Join - enrichment primitive
  - GroupBy - aggregation primitive
  - Source
  - Metadata (designed to be extensible, but we want to make sure our extensions are general and future proof)
- User facing Python tooling
  - compile.py
  - run.py
  - explore.py
- Java APIs
  - KVStore - kv store connectors are implemented against this (once per company)
  - Fetcher - this is used by applications to read processed data (used many many times)
  - Stats Store - used by Grafana dashboards
  - Metadata Store - used to manage metadata
  - Stream Decoder - used to implement connectors and decoders for streams

Not all compatibility commitments are the same. We need to spend significantly more time on public APIs as these can break code for existing users. They cause people to rebuild code and lead to compatibility issues in large multi-dependency projects (which end up requiring multiple incompatible versions). Configuration, monitoring, and command line tools can be faster and looser — changes here will break monitoring dashboards and require a bit of care during upgrades but aren't a huge burden.

For the most part monitoring, command line tool changes, and configs are added with new features so these can be done with a single CHIP.

## What should be included in a CHIP?

A CHIP should contain the following sections:

- Motivation: describe the problem to be solved
- Proposed Change: describe the new thing you want to do. This may be fairly extensive and have large subsections of its own. Or it may be a few sentences, depending on the scope of the change.
- New or Changed Public Interfaces: impact to any of the "compatibility commitments" described above. We want to call these out in particular so everyone thinks about them.
Migration Plan and Compatibility: if this feature requires additional support for a no-downtime upgrade describe how that will work
- Rejected Alternatives: What are the other alternatives you considered and why are they worse? The goal of this section is to help people understand why this is the best solution now, and also to prevent churn in the future when old alternatives are reconsidered.

## Who should initiate the CHIP?

Anyone can initiate a CHIP but you shouldn't do it unless you have an intention of doing the work to implement it.

## Process

Here is the process for making a CHIP:

1. Create a PR in chronon/proposals with a single markdown file.Take the next available CHIP number and create a file “CHIP-42 Monoid caching for online & real-time feature fetches”. This is the document that you will iterate on.
2. Fill in the sections as described above and file a PR. These proposal document PRs are reviewed by the committer who is on-call. They usually get merged once there is enough detail and clarity.
3. Start a [DISCUSS] issue on github. Please ensure that the subject of the thread is of the format [DISCUSS] CHIP-{your CHIP number} {your CHIP heading}. In the process of the discussion you may update the proposal. You should let people know the changes you are making.
4. Once the proposal is finalized, tag the issue with the “voting-due” label.  These proposals are more serious than code changes and more serious even than release votes. In the weekly committee meetings we will vote for/against the CHIP - where Yes, Veto-no, Neutral are the choices. The criteria for acceptance is 3+ “yes” vote count by the members of the committee without a veto-no. Veto-no votes require in-depth technical justifications to be provided on the github issue.
5. Please update the CHIP markdown doc to reflect the current stage of the CHIP after a vote. This acts as the permanent record indicating the result of the CHIP (e.g., Accepted or Rejected). Also report the result of the CHIP vote to the github issue thread.

It's not unusual for a CHIP proposal to take long discussions to be finalized. Below are some general suggestions on driving CHIP towards consensus. Notice that these are hints rather than rules. Contributors should make pragmatic decisions in accordance with individual situations.

- The progress of a CHIP should not be long blocked on an unresponsive reviewer. A reviewer who blocks a CHIP with dissenting opinions should try to respond to the subsequent replies timely, or at least provide a reasonable estimated time to respond.
- A typical reasonable time to wait for responses is 1 week, but be pragmatic about it. Also, it would be considerate to wait longer during holiday seasons (e.g., Christmas, Chinese New Year, etc.).
- We encourage CHIP proposers to actively reach out to the interested parties (e.g., previous contributors of the relevant part) early. It helps expose and address the potential dissenting opinions early, and also leaves more time for other parties to respond while the proposer works on the CHIP.
- Committers should use their veto rights with care. Vetos must be provided with a technical justification showing why the change is bad. They should not be used for simply blocking the process so the voter has more time to catch up.

# Resources

Below is a list of resources that can be useful for development and debugging.

## Docs

[Docsite](https://chronon.ai)\
[doc directory](https://github.com/airbnb/chronon/tree/main/docs/source)\
[Code of conduct](TODO)

## Links

[pip project](https://pypi.org/project/chronon-ai/)\
[maven central](https://mvnrepository.com/artifact/ai.chronon/): [publishing](https://github.com/airbnb/chronon/blob/main/devnotes.md#publishing-all-the-artifacts-of-chronon)\
[Docsite: publishing](https://github.com/airbnb/chronon/blob/main/devnotes.md#chronon-artifacts-publish-process)

## Code Pointers

### API

[Thrift](https://github.com/airbnb/chronon/blob/main/api/thrift/api.thrift#L180), [Python](https://github.com/airbnb/chronon/blob/main/api/py/ai/chronon/group_by.py)\
[CLI driver entry point for job launching.](https://github.com/airbnb/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/Driver.scala)

### Offline flows that produce hive tables or file output

[GroupBy](https://github.com/airbnb/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/GroupBy.scala)\
[Staging Query](https://github.com/airbnb/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/StagingQuery.scala)\
[Join backfills](https://github.com/airbnb/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/Join.scala)\
[Metadata Export](https://github.com/airbnb/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/MetadataExporter.scala)

### Online flows that update and read data & metadata from the kvStore

[GroupBy window tail upload](https://github.com/airbnb/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/GroupByUpload.scala)\
[Streaming window head upload](https://github.com/airbnb/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/streaming/GroupBy.scala)\
[Fetching](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Fetcher.scala)

### Aggregations

[time based aggregations](https://github.com/airbnb/chronon/blob/main/aggregator/src/main/scala/ai/chronon/aggregator/base/TimedAggregators.scala)\
[time independent aggregations](https://github.com/airbnb/chronon/blob/main/aggregator/src/main/scala/ai/chronon/aggregator/base/SimpleAggregators.scala)\
[integration point with rest of chronon](https://github.com/airbnb/chronon/blob/main/aggregator/src/main/scala/ai/chronon/aggregator/row/ColumnAggregator.scala#L223)\
[Windowing](https://github.com/airbnb/chronon/tree/main/aggregator/src/main/scala/ai/chronon/aggregator/windowing)

### Testing

[Testing - sbt commands](https://github.com/airbnb/chronon/blob/main/devnotes.md#testing)\
[Automated testing - circle-ci pipelines](https://app.circleci.com/pipelines/github/airbnb/chronon)\
[Dev Setup](https://github.com/airbnb/chronon/blob/main/devnotes.md#prerequisites)
