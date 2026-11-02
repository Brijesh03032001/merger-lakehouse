import re
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
BUNDLE = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
JOBS_PATH = ROOT / "resources/jobs.yml"
JOBS = yaml.safe_load(JOBS_PATH.read_text(encoding="utf-8"))["resources"]["jobs"]
DEPLOY_WORKFLOW = (ROOT / ".github/workflows/deploy-databricks.yml").read_text(encoding="utf-8")


def _tasks(job_name):
    return {task["task_key"]: task for task in JOBS[job_name]["tasks"]}


def test_bundle_defines_isolated_dev_and_prod_targets():
    assert BUNDLE["targets"]["dev"]["default"] is True
    assert BUNDLE["targets"]["dev"]["variables"]["catalog"] == "fmcg_dev"
    assert BUNDLE["targets"]["dev"]["variables"]["source_root"] == "/Volumes/fmcg_dev/bronze/landing"
    assert BUNDLE["targets"]["dev"]["variables"]["incremental_schedule_status"] == "PAUSED"
    assert BUNDLE["targets"]["prod"]["variables"]["catalog"] == "fmcg"
    assert BUNDLE["targets"]["prod"]["variables"]["incremental_schedule_status"] == "UNPAUSED"


def test_bundle_contains_manual_initial_and_scheduled_incremental_jobs():
    assert {"fmcg_prepare_storage", "fmcg_initial_load", "fmcg_daily_incremental"} <= set(JOBS)
    assert len(JOBS["fmcg_prepare_storage"]["tasks"]) == 1
    assert "schedule" not in JOBS["fmcg_initial_load"]
    schedule = JOBS["fmcg_daily_incremental"]["schedule"]
    assert schedule["quartz_cron_expression"] == "0 0 2 * * ?"
    assert schedule["timezone_id"] == "UTC"


def test_initial_load_dag_orders_dimensions_before_facts():
    tasks = _tasks("fmcg_initial_load")
    assert {item["task_key"] for item in tasks["load_pricing"]["depends_on"]} == {"load_products"}
    assert {item["task_key"] for item in tasks["load_historical_orders"]["depends_on"]} == {
        "create_date_dimension",
        "load_customers",
        "load_pricing",
    }


def test_every_notebook_task_points_to_a_versioned_source_file():
    for job in JOBS.values():
        for task in job["tasks"]:
            notebook = task["notebook_task"]
            resolved = (JOBS_PATH.parent / notebook["notebook_path"]).resolve()
            assert resolved.is_relative_to(ROOT)
            assert resolved.is_file(), resolved
            assert notebook["source"] == "WORKSPACE"


def test_all_data_tasks_receive_catalog_and_source_parameters():
    for job in JOBS.values():
        for task in job["tasks"]:
            parameters = task["notebook_task"].get("base_parameters", {})
            assert "catalog" in parameters
            if "data_source" in parameters:  # tasks that read landed files need the landing root
                assert parameters["source_root"] == "{{job.parameters.source_root}}"


def test_manual_deployment_uses_oidc_and_validates_before_deploying():
    assert "workflow_dispatch:" in DEPLOY_WORKFLOW
    assert "DATABRICKS_AUTH_TYPE: github-oidc" in DEPLOY_WORKFLOW
    assert "id-token: write" in DEPLOY_WORKFLOW
    assert DEPLOY_WORKFLOW.index("databricks bundle validate") < DEPLOY_WORKFLOW.index(
        "databricks bundle deploy"
    )



def test_every_job_parameter_a_task_references_is_declared():
    for name, job in JOBS.items():
        declared = {parameter["name"] for parameter in job.get("parameters", [])}
        for task in job["tasks"]:
            for value in task["notebook_task"].get("base_parameters", {}).values():
                for referenced in re.findall(r"\{\{job\.parameters\.(\w+)\}\}", str(value)):
                    assert referenced in declared, f"{name}/{task['task_key']} uses undeclared parameter {referenced}"


def test_existing_jobs_are_unchanged_by_the_new_stages():
    """The initial-load and daily jobs were verified on Free Edition; new stages live in new jobs."""
    assert [t["task_key"] for t in JOBS["fmcg_initial_load"]["tasks"]] == [
        "setup_catalog", "create_date_dimension", "load_customers", "load_products", "load_pricing",
        "load_historical_orders",
    ]
    assert [t["task_key"] for t in JOBS["fmcg_daily_incremental"]["tasks"]] == [
        "refresh_customers", "refresh_products", "refresh_pricing", "ingest_incremental_orders",
    ]


def test_autoloader_cannot_run_against_the_legacy_job_by_default():
    """Both read the same landing folder; cutover is an explicit pause/unpause."""
    assert BUNDLE["variables"]["autoloader_schedule_status"]["default"] == "PAUSED"
    for target in BUNDLE["targets"].values():
        assert target.get("variables", {}).get("autoloader_schedule_status", "PAUSED") == "PAUSED"
    assert JOBS["fmcg_orders_autoloader"]["schedule"]["pause_status"] == "${var.autoloader_schedule_status}"


def test_autoloader_ingestion_retries_once_so_schema_changes_restart_the_stream():
    ingest = _tasks("fmcg_orders_autoloader")["ingest_orders"]
    assert ingest["max_retries"] == 1
    assert {d["task_key"] for d in _tasks("fmcg_orders_autoloader")["quality_gate"]["depends_on"]} == {"ingest_orders"}


def test_alerting_routes_are_wired_to_one_variable():
    assert BUNDLE["variables"]["alert_emails"]["type"] == "complex"
    for name in ("fmcg_orders_autoloader", "fmcg_data_health", "fmcg_backfill_orders", "fmcg_entity_resolution"):
        assert JOBS[name]["email_notifications"]["on_failure"] == "${var.alert_emails}"
    rules = JOBS["fmcg_orders_autoloader"]["health"]["rules"]
    assert rules[0]["metric"] == "RUN_DURATION_SECONDS"


def test_health_job_runs_after_the_daily_load():
    autoloader = JOBS["fmcg_orders_autoloader"]["schedule"]["quartz_cron_expression"]
    health = JOBS["fmcg_data_health"]["schedule"]["quartz_cron_expression"]
    hour = lambda cron: int(cron.split()[2])
    assert hour(health) > hour(autoloader)
    assert BUNDLE["targets"]["prod"]["variables"]["health_schedule_status"] == "UNPAUSED"


def test_entity_resolution_is_off_for_external_llms_unless_asked():
    parameters = {p["name"]: p["default"] for p in JOBS["fmcg_entity_resolution"]["parameters"]}
    assert parameters["llm_enabled"] == "false"
    assert parameters["llm_model"] == "claude-opus-5-5"


def test_reporting_runs_only_after_both_matches_finish():
    tasks = _tasks("fmcg_entity_resolution")
    assert {d["task_key"] for d in tasks["build_reporting_views"]["depends_on"]} == {"resolve_customers", "resolve_products"}


def test_kafka_job_is_paused_everywhere_and_fails_fast_without_a_broker():
    assert BUNDLE["variables"]["kafka_schedule_status"]["default"] == "PAUSED"
    for target in BUNDLE["targets"].values():
        assert target.get("variables", {}).get("kafka_schedule_status", "PAUSED") == "PAUSED"
    assert BUNDLE["variables"]["kafka_bootstrap_servers"]["default"] == "", "an unset broker must not default to a real host"
    job = JOBS["fmcg_orders_kafka"]
    assert job["max_concurrent_runs"] == 1 and job["email_notifications"]["on_failure"] == "${var.alert_emails}"
    task = _tasks("fmcg_orders_kafka")["ingest_orders_from_kafka"]
    assert task["max_retries"] == 1
    assert task["notebook_task"]["base_parameters"]["trigger"] == "availableNow", "scheduled runs drain and stop"


def _keys(node):
    if isinstance(node, dict):
        for key, value in node.items():
            yield str(key)
            yield from _keys(value)
    elif isinstance(node, list):
        for item in node:
            yield from _keys(item)


def _strings(node):
    if isinstance(node, dict):
        for value in node.values():
            yield from _strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from _strings(item)
    elif isinstance(node, str):
        yield node


def test_no_kafka_credential_is_ever_written_into_the_bundle():
    """Credentials come from a secret scope at run time. The bundle holds only the scope's name."""
    for tree in (BUNDLE, JOBS):
        assert not [k for k in _keys(tree) if re.search(r"password|username|jaas|api_key|token", k, re.I)]
        assert not [v for v in _strings(tree) if re.search(r"LoginModule|sasl\.jaas|password=", v, re.I)]
    assert BUNDLE["variables"]["kafka_secret_scope"]["description"].startswith("Secret scope")


def test_every_health_gate_default_lists_the_same_bronze_tables():
    expected = "orders,orders_raw,orders_kafka"
    for name in ("fmcg_orders_autoloader", "fmcg_backfill_orders", "fmcg_data_health"):
        defaults = {p["name"]: p["default"] for p in JOBS[name]["parameters"]}
        assert defaults["bronze_tables"] == expected, name
    gate = (ROOT / "consolidated_pipeline/5_quality/1_run_quality_checks.py").read_text(encoding="utf-8")
    checks = (ROOT / "consolidated_pipeline/common/quality_checks.py").read_text(encoding="utf-8")
    assert f'"bronze_table", "{expected}"' in gate
    assert f'bronze_table: str = "{expected}"' in checks
