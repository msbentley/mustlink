# mustlink
A python wrapper for the WebMUST API (mustlink)

This is a simple wrapper in python for the [WebMUST](https://www.esa.int/Enabling_Support/Operations/WebMUST_br_A_web-based_client_for_MUST) API.

## Dependencies

The following dependencies must be met:
- python 3
- matplotlib
- pandas
- pyyaml
- requests

## Installation

### pip

```pip install mustlink```

should do the job, although creating a dedicated environment is recommended (see below).

### conda

First, clone this repository. If you are using conda, the dependencies can be installed in a new environment using the provided environment file:

```conda env create -f environment.yml```

The newly created environment can be activated with:

```conda activate mustlink```

Otherwise, please make sure the dependencies are installed with your system package manager, or a tool like `pip`. Use of a conda environment or virtualenv is recommended!

The package can then be installed with:

```python setup.py install```


## URL

The URL for the WebMUST instance in use can be specified when instantiating the Must class. If none is given, a default URL is used. For example:

```python
must = mustlink.Must(url='https://mustinstance.com/mustlink')
```

## Authentication

Access to WebMUST needs authentication. This is controlled by a config file which can be pointed to by the `config_file` parameter when instantiating the Must class, for example:

```python
must = mustlink.Must(config_file='path_to/config.file')
```

If nothing is specified, a file `mustlink.yml` is looked for in paths pointed to by the environment variables `APPDATA`, `XDG_CONFIG_HOME` or in the `.config` folder in the user's home directory.

The configuration file should be in YAML format and contain the username and password as follows:

```yaml
user:
    login: "userone"
    password: "blah"
```

## Example

The Jupyter notebook included with this repository shows an example of how to use the code. Note that not all API functions are wrapped by this library, but only those that are commonly used. To view the notebook, click [here](https://nbviewer.jupyter.org/github/msbentley/mustlink/blob/master/mustlink_example.ipynb).
