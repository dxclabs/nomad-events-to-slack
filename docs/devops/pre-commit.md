### Pre Commit

Here's a useful reference:
https://pre-commit.com/


install pre-commit
```
python -m pip install pre-commit
```
or
```
python -m pip install -r requirements/requirements-dev.txt
```

Make sure you have the linters install (black, bandit, isort etc)

install git hooks with:
```
pre-commit install
```

Test with:
```
pre-commit run --all-files
```

Update pre-commit config and hooks with:
```
pre-commit autoupdate
```
