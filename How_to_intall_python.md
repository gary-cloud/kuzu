# How to install python

```bash
make clean-python-api
make python NUM_THREADS=$(nproc)

# `package_tar.py` is based on `git archive HEAD`, so only committed changes are included in the package.
# If there are uncommitted modifications, commit them first or modify the script to package the current working tree.
cd scripts/pip-package
python3 package_tar.py /tmp/kuzu-custom.tar.gz
pip install /tmp/kuzu-custom.tar.gz
```