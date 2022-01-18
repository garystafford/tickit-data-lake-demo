# Installation instructions

Install git hooks and Python requirements.

```shell
cp git_hooks/* .git/hooks
chmod +x .git/hooks/pre-commit
chmod +x .git/hooks/pre-push

python3 -m pip install --user -U -r requirements/requirements.txt
python3 -m pip install --user -U -r requirements/requirements_local_tests.txt
```