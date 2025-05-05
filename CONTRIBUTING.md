# Contributing to mcp-server

Thank you for your interest in contributing to mcp-server! This project provides tools for integrating Keboola data with AI assistants through the Model Context Protocol. We welcome contributions of all kinds, from bug fixes and feature additions to documentation improvements.

## Code of Conduct

This project follows the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). By participating, you agree to uphold this code. Please report unacceptable behavior to the project maintainers.

## How to Contribute

### Reporting Issues

If you find a bug or have a suggestion for improving the project:

1. Check if the issue already exists in the [issue tracker](https://github.com/keboola/mcp-server/issues)
2. If not, create a new issue, providing as much relevant information as possible
3. Include steps to reproduce bugs, or clear descriptions of the proposed feature

### Pull Requests

1. Fork the repository
2. Create a new branch for your feature or bug fix: `git checkout -b feature/your-feature-name`
3. Make your changes
4. Ensure your code follows the project's style guidelines and passes all tests
5. Submit a pull request with a clear description of the changes

### Development Setup

1. Clone the repository
2. Install dependencies:
   ```
   pip install -e ".[dev]"
   ```

## Code Style

- Follow [PEP 8](https://pep8.org/) standards
- Use 4 spaces for indentation
- Run `black` and `isort` before committing changes

## Testing

- Add tests for new features or bug fixes
- Ensure all tests pass before submitting a pull request
- Run tests with:
  ```
  pytest
  ```

## Documentation

- Update documentation for any changed functionality
- Use Google-style docstrings for Python code

## License

By contributing to mcp-server, you agree that your contributions will be licensed under the project's MIT License.

## Questions?

If you have any questions, feel free to open an issue or contact the maintainers.

Thank you for contributing to make our mcp-server better!