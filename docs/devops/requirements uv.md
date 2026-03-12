# Requirements for UV Installation

Use Poetry for dependency resolution

Requires plugin
```
[tool.poetry.requires-plugins]
poetry-plugin-export = ">=1.8"
```

## requirements.txt is used for app only.
Create or update requirements.txt like this:
```
poetry export --without-hashes --with types -f requirements.txt -o requirements.txt
```

## requirements.tests.txt used with requirements.txt for pytest
```
poetry export --without-hashes --only tests -f requirements.txt -o requirements.tests.txt
```

## requirements.linters.txt used alone for linting
```
poetry export --without-hashes --only linters -f requirements.txt -o requirements.linters.txt
```
