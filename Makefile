pip-sync:
	pip-sync requirements.txt

pip-compile:
	pip-compile requirements.in --resolver=backtracking