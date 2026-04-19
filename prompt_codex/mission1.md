Read this repository first.
Then read PROJECT_RULES.md.
Then run the project quality gate:
sudo docker compose exec web python tools/run_quality_gate.py

Do not modify anything yet.
I want:
1. the failing tests
2. the exact root cause hypothesis
3. the minimal files to patch
4. a proposed plan before any code change
