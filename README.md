[![PyPI package](https://img.shields.io/pypi/v/datanator_query_python.svg)](https://pypi.python.org/pypi/datanator_query_python) 
[![Documentation](https://readthedocs.org/projects/datanator-query-python/badge/?version=latest)](https://docs.karrlab.org/datanator_query_python)
[![Test results](https://circleci.com/gh/KarrLab/datanator_query_python.svg?style=shield)](https://circleci.com/gh/KarrLab/datanator_query_python)
[![Test coverage](https://coveralls.io/repos/github/KarrLab/datanator_query_python/badge.svg)](https://coveralls.io/github/KarrLab/datanator_query_python)
[![Code analysis](https://api.codeclimate.com/v1/badges/141d01cec991d17ed5ab/maintainability)](https://codeclimate.com/github/KarrLab/datanator_query_python)
[![License](https://img.shields.io/github/license/KarrLab/datanator_query_python.svg)](LICENSE)
![Analytics](https://ga-beacon.appspot.com/UA-86759801-1/datanator_query_python/README.md?pixel)

# Datanator-query-python
Python query interface of [Datanator](https://datanator.info), a toolkit for discovering the data needed for modeling the biochemistry of cells.

## Contents
* [Overview](#overview)
* [Usage and installation](#usage-and-installation)
* [License](#license)
* [Development team](#development-team)
* [Questions and comments](#questions-and-comments)

## Overview
A central goal of synthetic biology is to rationally design organisms. Similarly, a central goal of precision medicine is to tailor therapy to each patient based on their unique genome. Many engineering fields use computer-aided design (CAD) tools driven by mechanistic models to efficiently design complex systems such as planes. Analogously, more comprehensive and more predictive models of biological systems, such as [whole-cell models](https://www.wholecell.org), are needed to help bioengineers and physicians design biomachines and medical therapy.

One of the biggest bottlenecks to achieving such models is collecting and aggregating the large amount of data needed for model construction and verification. Due to advances in genomics and increased emphasis on data sharing, there is now extensive data about a wide range of biochemical entities and processes such as data about metabolite concentrations, RNA and protein abundances, and reaction rates. However, it remains difficult to utilize this information for modeling because the data scattered across numerous databases and publications; the data is described using different formats, identifiers, and units; and there are inadequate tools for finding relevant data for modeling a specific biological system in a specific environment.

The *Datanator* toolkit seeks to address these problems for biochemical modeling by providing investigators an integrated database of molecular data and tools for discovering relevant data for modeling projects and other meta analyses. Please see the [About](https://datanator.info/about) page for more information about the goals and features of *Datanator*.

The *Datanator* toolkit is composed of the following packages:
- [*Datanator*](https://github.com/KarrLab/datanator): Tools for aggregating and integrating diverse data from diverse sources into a single dataset and searching these datasets
- *Datanator-db*: MongoDB server for *Datanator-data*
- *Datanator-fulltext-db*: ElasticSearch server for *Datanator-data*
- [*Datanator-query-python*](https://github.com/KarrLab/datanator_query_python): Tools for querying *Datanator-db* and *Datanator-fulltext-db*
- [*Datanator-rest-api*](https://github.com/KarrLab/datanator_rest_api): REST interface for *Datanator-query-python*
- *Datanator-frontend*: This package, a web-based graphical user interface to *Datanator-db*.

This package provides a pythonic interface to programmatically access data stored in *Datanator-db* and *Datanator-fulltext-db*.

## Testing, installation, and usage

### Users
We recommend using the [REST API](https://api.datanator.info) to programmatically access data stored in *Datanator-db* and *Datanator-fulltext-db*.

### Developers

#### Install *Datanator-query-python*
We presume Python 3.7 has already been installed.

1. Install `git`
  ```
  apt-get install git
  ```

2. Clone this repository
  ```
  git clone https://github.com/karrlab/datanator_query_python
  ```

4. Install this package
  ```    
  cd /path/to/datanator_query_python
  pip install -r requirements.txt
  pip install -e .
  ```

5. Setup environment variables (credentials for accessing databases)
  ```
  export USERNAME=XXXXXXX
  export PASSWORD=XXXXXXX
  export SERVER=XXXXXXX
  export AUTHDB=XXXXXXX
  export READ_PREFERENCE=XXXXXX
  ```

#### Test *Datanator-query-python*
1. Run the unit tests
  ```
  python -m pytest test-unit-coverage
  ```

### File organization
This repository is organized as follows:

- `datanator_query_python/`:  
  - `aggregate/`: aggregation pipelines for MongoDB
  - `config/`: datanator_query_python configuration
  - `query/`: collection-based query scripts
  - `query_schema_2/`: data-type-based query scripts
  - `util/`: utility tools
- `tests/`
  - `aggregate/` - test scripts in `aggregate` directory
  - `config/` - test scripts in `config` directory
  - `query/` - test scripts in `query` directory
  - `util/` - test scripts in `util` directory
- `CODE_OF_CONDUCT`: code of conduct
- `LICENSE`: license
- `requirements.txt`: package configuration
- `README`: overview of the application

  
## Development and deployment workflow
1. Create a Git new branch
2. Commit code to the new branch
3. Push the branch to GitHub
4. GitHub will automatically trigger [CircleCI](https://circleci.com/gh/KarrLab/datanator_query_python) to run the unit test, integration tests, and other static analyses
5. Use [Coveralls](https://coveralls.io/github/KarrLab/datanator_query_python) to review the coverage of the tests
6. As needed, add additional tests and fix any failing tests
7. Once the new code passes the tests and has high coverage, create a [pull request](https://github.com/KarrLab/datanator_query_python/compare) to merge the new branch into the master branch
7. One of the main developers will review the pull request and request changes as necessary
9. Once any necessary changes have been made, one of the main developers will approve the pull request
10. GitHub will automatically trigger [CircleCI](https://circleci.com/gh/KarrLab/datanator_query_python) to run the unit test, integration tests, and other static analyses for the master branch
11. Once the CircleCI build succeeds, downstream dependencies will automatically be tested and built

## Contributing to *Datanator*
We welcome contributions to *Datanator* via Git pull requests. Please contact the developers to coordinate potential contributions, and please see above for information about how to submit pull requests.

## License
This package is released under the [MIT license](LICENSE). The licenses of the third party dependencies are summarized in [LICENSE-THIRD-PARTY](LICENSE-THIRD-PARTY).

## Development team
This package was developed by the [Karr Lab](https://www.karrlab.org) at the Icahn School of Medicine at Mount Sinai in New York by the following individuals:

* [Yosef Roth](https://www.linkedin.com/in/yosef-roth-a80a378a)
* [Yang Lian](https://www.linkedin.com/in/zlian/)
* [Jonathan Karr](https://www.karrlab.org)
* [Saahith Pochiraju](https://www.linkedin.com/in/saahithpochiraju/)
* [Bilal Shaikh](https://www.linkedin.com/in/bilalshaikh42/)
* Balazs Szigeti

## Questions and comments
Please submit an [issue](https://github.com/KarrLab/datanator_query_python/issues/new), or contact the [Karr Lab](info@karrlab.org) with any questions or comments.

## Acknowledgements
Datanator was developed with support from the [Center for Reproducible Biomedical Modeling](https://reproduciblebiomodels.org) from the National Institute of Bioimaging and Bioengineering and the National Institute of General Medical Sciences of the National Institutes of Health and the National Science Foundation (awards P41EB023912 and R35GM119771).
