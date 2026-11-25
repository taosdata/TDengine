# TDgpt AI Skills

This directory contains agent skills published with TDgpt.

| Skill | Description |
| --- | --- |
| [tdgpt-custom-algorithm](tdgpt-custom-algorithm/SKILL.md) | Deploy user-defined anomaly detection (`ad`), forecast (`fc`) and batch envelope (`batch`) algorithms. Generates an Adapter when the user model does not follow the TDgpt class hierarchy. |

## Using a skill

Point the AI agent at the skill's `SKILL.md`, or open the skill directory in an
editor that discovers `SKILL.md` files. Each skill is self-contained:

```text
tdgpt-custom-algorithm/
├── SKILL.md
├── scripts/deploy_custom_algorithm.py
└── templates/
```

`scripts/deploy_custom_algorithm.py` uses only the Python standard library and
supports native, Docker and SSH deployments. It never installs packages without
the user's explicit confirmation.

## Requirements

- `ad` / `fc` / `batch` user algorithms are supported.
- Execution host: Linux/macOS. The deployment script relies on a POSIX shell
  (`sh`, `chmod`, `sha256sum`, `base64`). Windows native and local Docker
  deployments are rejected; a Windows host can only use
  `--deploy-mode remote` against a Linux target.
- Target TDgpt main venv is detected automatically or supplied with
  `--venv-python`.
- Docker deployments must use a persistent bind mount or named volume for
  `custom_algo_dir`; the script refuses to write into an unmounted container
  path unless `--allow-ephemeral` is set.
