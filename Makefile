virtualenv/: requirements.txt
	/usr/apps/python3/bin/virtualenv virtualenv/
	source ./virtualenv/bin/activate \
		&& pip install --upgrade pip \
		&& pip install -r requirements.txt

install: virtualenv README.md LICENSE setup.py sonar_driver/*.py
	source ./virtualenv/bin/activate \
		&& pip install --upgrade --force-reinstall `pwd`

