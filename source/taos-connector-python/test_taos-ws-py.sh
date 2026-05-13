pip3 install poetry==1.8.5
poetry install --no-interaction --with=dev
poetry run pip install "numpy<2.0.0" taos-ws-py
export TDENGINE_URL=localhost:6041
curl -L -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" -d "show databases" localhost:6041/rest/sql
poetry run pytest ./taos-ws-py/tests/
