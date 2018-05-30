PYTHON=/bin/python3.4

venv/: requirements.txt
	${PYTHON} -m venv venv/
	source ./venv/bin/activate \
		&& pip install --upgrade pip \
		&& pip install -r requirements.txt

install: venv/ README.md LICENSE setup.py sonar_driver/*.py
	source ./venv/bin/activate \
		&& pip install --upgrade --force-reinstall `pwd`

