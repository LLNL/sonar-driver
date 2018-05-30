PYTHON_HOME=/Users/gimenez1/.pyenv/versions/3.6.5/

venv/: requirements.txt
	${PYTHON_HOME}/bin/python3 -m venv venv/
	source ./venv/bin/activate \
		&& pip install --upgrade pip \
		&& pip install -r requirements.txt

install: venv README.md LICENSE setup.py sonar_driver/*.py
	source ./venv/bin/activate \
		&& pip install --upgrade --force-reinstall `pwd`

