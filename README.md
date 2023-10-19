# data-manager

manage a central database of EOCIS bundles, datasets, variables, jobs and tasks

## Glossary

### Bundles

A `Bundle` is a logical collection of one or more datasets which can be retrieved together.  

* A bundle also includes a spatial extent expressed in the relevant coordinate system (typically WGS84) 
* DataSets within a bundle should align to a common grid
* 
### DataSets

A `DataSet` is a collection of spatio-temporal data files containing one or more variables.

* Each DataSet includes a temporal extent and resolution

### Variables

A `Variable` identifies a particular spatio-temporal observation within a DataSet

### Jobs

A `Job` represents a user request to retrieve data from one or more variables/datasets which belong to a bundle.

### Tasks

A `Task` represents a data-retrieval operation, performed as part of a `Job`.  Tasks can be executed in parallel.

## Setup

```
pip install -e .
(cd scripts; ./create.sh)
python -m eocis_data_manager.tools.populate_schema schema
python -m eocis_data_manager.tools.update_end_date
python -m eocis_data_manager.tools.dump
```