# mustlink
A python wrapper for the WebMUST API (mustlink)

This is a simple wrapper in python for the [WebMUST](https://www.esa.int/Enabling_Support/Operations/WebMUST_br_A_web-based_client_for_MUST) API.

## URL

The URL for the WebMUST instance in use can be specified when instantiating the Must class. If none is given, a defauly URL is used.

## Authentication

Access to WebMUST needs authentication. This is controlled by a config file which can be pointed to by the `config_file` parameter when instantiating the Must class. If nothing is specified, a file `mustlink.yml` is looked for. 

The configuration file should be in YAML format and contain the username and password as follows:

```yaml
user:
    login: userone
    password: blah
```

## Example

The Jupyter notebook included with this repository shows an example of how to use the code. Note that not all API functions are wrapped by this library, but only those that are commonly used.
