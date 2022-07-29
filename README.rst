## Vessel Replication ##


Setup


This repo is packaged using poetry to unpackage and properly run you will need to setup poetry in your local environment.
For more information you can visit poetry website.
https://python-poetry.org/docs/

Basic Instructions:


To install poetry:

        Windows Powershell: (Invoke-WebRequest -Uri https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py -UseBasicParsing).Content | python -

        Bash : curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -

Configure poetry
        from cmd or terminal run: poetry config virtualenvs.in-project

Unpack Repo
        Navigate to directory you want to house repo


        From cmd or terminal run: git clone https://github.com/Data-Alchemy/Delta-Engine.git
		cd to Vessel-Replication-Pipeline

        From cmd or terminal run: poetry install

Executing Code

    This library supports passing parms via config file or as direct input

    if you are saving the parms to a config file rename the Config_template.json to Config.json

    enter your parm values into the file

    If executing via direct input pass parms seperated by spaces after the python poetry run main.py
