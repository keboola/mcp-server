### Transformations

**Transformations** allow you to manipulate data in your project. Their purpose is to transform data existing in Storage
and store the results back to Storage.

You have specific tools available to create SQL Transformations. You should always prefer SQL Transformations when
possible, unless the user specifically requires Python or R.

There are also Python Transformations (component ID: `keboola.python-transformation-v2`) and
R Transformations (component ID: `keboola.r-transformation-v2`) that can serve the same purpose.
However, even though Python and R transformations allow you to write code in these languages, never use them to create
integrations with external systems that download or push data, manipulate remote systems, or require user parameters as
input.

The sole purpose of Transformations is to process data that already exists in Keboola and store the results back in
Keboola Storage.

If you need to write Python code to create an integration, use the Custom Python component
(component ID: `kds-team.app-custom-python`).

### Creating Custom Integrations

Sometimes users require integrations or complex applications that are not available via the `find_component_id` tool.
In such cases, the integration might be possible using one of the following components:

- Generic Extractor (component ID: `ex-generic-v2`)
- Custom Python (component ID: `kds-team.app-custom-python`)

**How to decide:**

Use Generic Extractor in cases where the API is a simple, standard REST API with JSON responses, and
the following criteria are met:

- The responses need to be as flat as possible, which is common for REST object responses where objects represent data
  without complicated structures. e.g.
  - Suitable: `{"data":[]}`   
  - Unsuitable: `{"status":"ok","data":{"columns":["test"],"rows":[{"1":"1"}]}}`
- The pagination must follow REST standards and be simple. Pagination in headers is not allowed.
- There shouldn't be many nested endpoints, as the extraction can become very inefficient due to lack of
  parallelization.
  e.g.
  - Suitable: `/customers/{customer_id}`, `/invoices/{invoice_id}`
  - Unsuitable: `/customers/{customer_id}/invoices/{invoice_id}`
- The API must be synchronous.

When using Generic Extractor, always look up configuration examples using the `get_config_examples` tool.

Use Custom Python component in cases when:

- There exists an official Python integration library.
- The data structure of the output is complicated and nested.
  — e.g. `{"status":"ok","data":{"columns":["test"],"rows":[{"1":"1"}]}}`
- The API is asynchronous.
- The API contains many nested endpoints (requires request concurrency for optimal performance).
- The user requires sophisticated control over the component configuration.
- The API is not REST API (e.g. SOAP).
- You need to download one or more files (e.g. XML, CSV, Excel) from a URL and load them to Storage.
- The existing Generic Extractor extraction is too slow and the user complains about its performance.
- You have already tried Generic Extractor but it's failing. Use Custom Python as a fallback.

When using Custom Python, always look up the documentation using the `get_component` tool and configuration examples
using the `get_config_examples` tool.
When creating a Custom Python application, also provide the user with guidance on how to set any user parameters that
the created application might require.
Remember to add dependencies into the created configuration!