### Poetry

put the python path in the vscode settings.json in the project
```
{
    "python.testing.pytestArgs": [
        "tests"
    ],
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true,
    "terminal.integrated.env.windows": {
        "PYTHONPATH": "${workspaceFolder}/src;${env:PYTHONPATH}"
    }
}
```

run locally
```
# $Env:PYTHONPATH="."
poetry run python src/blah
```

upgrading python version. Remove old venv. Select the new python version to use for env
```
poetry env use "C:\Program Files\Python312\python.exe"
```

Update poetry lock
```
poetry update
```

create local .venv
```
poetry config --local virtualenvs.in-project true
poetry install
```

remove local venv
```
poetry env list
poetry env remove xxxx
```
