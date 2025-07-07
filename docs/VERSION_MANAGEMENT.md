# Version Management

This project automatically updates the version in `pyproject.toml` when a new git tag is created.

## How It Works

When you create a git tag with the format `v1.2.3`, the GitHub Actions workflow will:

1. Extract the version number from the tag (removing the `v` prefix)
2. Update the `version` field in `pyproject.toml`
3. Commit and push the changes back to the repository
4. Continue with the Docker build and push process

## Usage

### Creating a New Release

1. **Create and push a new tag:**
   ```bash
   git tag v1.2.3
   git push origin v1.2.3
   ```

2. **The workflow will automatically:**
   - Update `pyproject.toml` version to `1.2.3`
   - Commit the change with message: `chore: update version to 1.2.3 [skip ci]`
   - Build and push the Docker image

### Manual Version Update

If you need to update the version manually, change [project] version field in `pyproject.toml`:

## Version Format

The version must follow semantic versioning format:
- `1.2.3` (major.minor.patch)
- `1.2.3-alpha.1` (with pre-release suffix)
- `1.2.3+build.1` (with build metadata)

## Workflow Details

The automatic version update happens in the `.github/workflows/release.yml` workflow:

1. **Trigger:** Push of tags matching `v*` pattern
2. **Version Extraction:** Removes `v` prefix from git tag
3. **File Update:** Uses the reusable action `.github/actions/update-version`
4. **Validation:** Ensures version format is valid
5. **Commit:** Automatically commits and pushes the change

## Troubleshooting

### Version Not Updated
- Ensure the tag format is correct (e.g., `v1.2.3`, not `1.2.3`)
- Check that the workflow has `contents: write` permissions
- Verify the tag was pushed to the repository

### Invalid Version Format
- The workflow validates semantic versioning format
- Pre-release and build metadata are supported
- Examples of valid versions: `1.2.3`, `1.2.3-alpha.1`, `1.2.3+build.1`

### Manual Override
If you need to manually set a version that doesn't match the tag:

## Files Modified

- `.github/workflows/release.yml` - Main workflow that triggers on tags
- `.github/actions/update-version/action.yml` - Reusable action for version updates
- `pyproject.toml` - Target file that gets updated 