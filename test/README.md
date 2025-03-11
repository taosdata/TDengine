pip install uv
uv init
uv sync --frozen
pytest cases/xxx/xxx/xxx.py
    -N 5 -M 3 
    --yaml_file default.yaml
    --debug_log

