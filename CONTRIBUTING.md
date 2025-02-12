# Contributing to Mosaic

## Overview
We happily welcome contributions to Mosaic package. We use [GitHub Issues][https://github.com/databrickslabs/mosaic/issues] to track community reported issues and [GitHub Pull Requests][https://github.com/databrickslabs/mosaic/pulls] for accepting changes.
Contributions are licensed on a license-in/license-out basis.

# Contributing Guide

## Communication
Before starting work on a major feature, please reach out to us via GitHub, Slack, email, etc. We will make sure no one else is already working on it and ask you to open a GitHub issue.
A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.
We will use the GitHub issue to discuss the feature and come to agreement.
This is to prevent your time being wasted, as well as ours.
The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.
If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location.
Specifically, if the goal is to add a new extension, please read the extension policy.
Small patches and bug fixes don't need prior communication.

## Coding Style
We follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) with one exception: lines can be up to 100 characters in length, not 79.
We use [scalafmt](https://github.com/databrickslabs/mosaic/blob/main/.scalafmt.conf) to format our Scala code. Please run scalafmt on your code before submitting a pull request.

## Sign your work
The sign-off is a simple line at the end of the explanation for the patch. Your signature certifies that you wrote the patch or otherwise have the right to pass it on as an open-source patch. The rules are pretty simple: if you can certify the below (from developercertificate.org):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

Then you just add a line to every git commit message:

```
Signed-off-by: Joe Smith <joe.smith@email.com>
Use your real name (sorry, no pseudonyms or anonymous contributions.)
```

If you set your `user.name` and `user.email` git configs, you can sign your commit automatically with git commit -s.

## Repository structure
The repository is structured as follows:

- `pom.xml` Mosaic project definition and dependencies 
- `src/` Scala source code and tests for Mosaic
- `python/` Source code for Python bindings
- `docs/` Source code for documentation
- `.github/workflows` CI definitions for Github Actions

## Test & build Mosaic

Given that DBR 13.3 is Ubuntu 22.04, we recommend using docker, 
see [mosaic-docker.sh](https://github.com/databrickslabs/mosaic/blob/main/scripts/docker/mosaic-docker.sh).

### Scala JAR

We use the [Maven](https://maven.apache.org/install.html) build tool to manage and build the Mosaic scala project.

The Mosaic JAR including all dependencies can be generated by running: `mvn clean package`.
By default, this will also run the tests in `src/test/`.

The packaged JAR should be available in `target/`.

### Python bindings

The python bindings can be tested using [unittest](https://docs.python.org/3/library/unittest.html).
- Build the scala project and copy to the packaged JAR to the `python/mosaic/lib/` directory.
- Move to the `python/` directory and install the project and its dependencies:
    `pip install . && pip install pyspark==<project_spark_version>`
  (where 'project_spark_version' corresponds to the version of Spark 
  used for the target Databricks Runtime, e.g. `3.4.1` for DBR 13.3 LTS.
- Run the tests using `unittest`: `python -m unittest`

The project wheel file can be built with [build](https://pypa-build.readthedocs.io/en/stable/).
- Install the build requirements: `pip install build wheel`.
- Build the wheel using `python -m build`.
- Collect the .whl file from `python/dist/`

### Documentation

The documentation has been produced using [Sphinx](https://www.sphinx-doc.org/en/master/).

To build the docs:
- Install the pandoc library (follow the instructions for your platform [here](https://pandoc.org/installing.html)).
- Install the python requirements from `docs/docs-requirements.txt`.
- Build the HTML documentation by running `make html` from `docs/`.
  - For nbconvert you may have to symlink your jupyter share folder, 
    e.g. `sudo ln -s /opt/homebrew/share/jupyter /usr/local/share`. 
- You can locally host the docs by running the `reload.py` script in the `docs/source/` directory.

## Style

Tools we use for code formatting and checking:
- `scalafmt` and `scalastyle` in the main scala project.
- `black` and `isort` for the python bindings.
