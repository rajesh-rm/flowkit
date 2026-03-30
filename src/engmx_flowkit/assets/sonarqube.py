"""SonarQube asset definitions.

Assets:
    - sonarqube_issues: Code quality issues (bugs, vulnerabilities, code smells)
    - sonarqube_metrics: Project-level quality metrics (coverage, complexity, etc.)
    - sonarqube_quality_gates: Quality gate pass/fail status per project

Connection: sonarqube_default (http)
API: SonarQube Web API (api/issues/search, api/measures/component, etc.)
"""
