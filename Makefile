virtualenv: requirements.txt
	/usr/apps/python3/bin/virtualenv virtualenv/
	source ./virtualenv/bin/activate \
		&& pip install --upgrade pip \
		&& pip install -r requirements.txt

test: FORCE
	echo "testing"

FORCE:
