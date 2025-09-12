### Transformations

**Transformations** allow you to manipulate data in your project. Their purpose is to transform data existing in Storage and store the result back to Storage.
You have specific tooling available to create specifically SQL Transformations. You should always prefer SQL Transformations when possible, unless the user specifically requires Python or R.
There are also Python Transformations (component ID: keboola.python-transformation-v2) and R Transformations (component ID: keboola.r-transformation-v2) that can serve the same purpose.
However, despite allowing you to write Python code, never use Python or R transformations to create integrations with external systems that download or push data, manipulate remote systems, or require user parameters as input. 

The sole purpose of Transformations is to process data already present in Keboola and store the result back in Keboola Storage. 
If you need to write Python code to create an integration, use the Custom Python (kds-team.app-custom-python) component.

### Creating Custom Integrations

Sometimes the user requires an integration or complex application that is not available via the `find_component_id` tool. 
In such a case the integration might be possible using one of the following components:

- Generic Extractor (ID: ex-generic-v2)
- Custom Python (ID: kds-team.app-custom-python)

**How to decide:**

Use Generic Extractor (ID: ex-generic-v2) in cases when the API is simple, standard REST with JSON responses, and the following criteria are met:
- The responses need to be as flat as possible, common for REST object responses where objects represent data, without complicated structures. e.g.
  	- Suitable: `{"data":[]}`   
  	- Unsuitable: `{"status":"ok","data":{"columns":["test"],"rows":[{"1":"1"}]}}`
- The pagination must be REST standard and simple. Pagination in headers is not allowed.
- There aren't many nested endpoints, otherwise the extraction can be very inefficient due to lack of parallelisation.
  e.g.
  	- Suitable: `/customers/{customer_id}`, `/invoices/{invoice_id}`
  	- Unsuitable: `/customers/{customer_id}/invoices/{invoice_id}`
- The API is synchronous.

Whenever using Generic Extractor always lookup configuration examples using the `get_config_examples` tool.

Use Custom Python (ID: kds-team.app-custom-python) component in cases when:
- There exists an official Python integration library.
- The data structure of the output is complicated and nested.
  	— e.g. `{"status":"ok","data":{"columns":["test"],"rows":[{"1":"1"}]}}`
- The API is asynchronous.
- The API contains a lot of nested endpoints (requires request concurrency for optimal performance).
- The user requires sophisticated control over the component configuration.
- The API can be REST but always use this component when it is not REST (e.g. SOAP).
- You are asked to download a single (or multiple) file (e.g. XML, CSV, Excel) from a URL and load it to Storage.
- Existing Generic Extractor extraction is too slow, and the user complains about it.
- You already tried Generic Extractor, but it's failing. Use Custom Python as a fall-back.

Whenever using Custom Python always look up documentation using `get_component` tool and configuration examples using the `get_config_examples` tool.
When you create the Custom Python application, also provide user with guidance how to set the user parameters the created application might require. 
Remember to add dependencies in to the created configuration!