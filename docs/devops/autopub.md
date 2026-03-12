# autopub
https://pypi.org/project/autopub/


### sample autopub config in pyproject.toml
```
[tool.autopub]
project-name = "gcuk-inbound"
git-username = ""
git-email = ""
changelog-file = "docs/changelog.rst"
changelog-header = "###############"
version-header = "="
version-strings = ["app/__init__.py"]
build-system = "poetry"
```

### set your own github user ###
Either in .git/config or globally, set your own commit user
```
git config --global user.email "user@email.com"
git config --global user.name "User Name"
```

### Add changes in RELEASE.md file
example:
```
Release type: minor

ci: update pre-commit
build: update poetry requirements
build: update npm requirements
feat: Add function to count widgets
test: fix test case for foo bar
docs: update autopub.md docs
```

### Use semantic commit messages
eg
https://gist.github.com/joshbuchea/6f47e86d2510bce28f8e7f42ae84c716


### Execute autopub
To be installed into github actions
```
autopub check
autopub prepare
# autopub build
autopub commit
autopub githubrelease
# autopub publish
```
