
## Build the distribution

```bash
# istall build tools
uv pip install --system hatchling build
# build
python -m build
# install twine
uv pip install --system twine
# upload to TestPyPI
twine upload --repository testpypi dist/*

```
