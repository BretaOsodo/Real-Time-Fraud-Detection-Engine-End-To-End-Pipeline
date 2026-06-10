"""
Integratio tests for Prometheus and grafana monitoring stack

"""

import pytest
import requests 
import time 
import socket

PROMETHEUS_URL = "http://localhost:9094"
GRAFANA_URL = "http://localhost:3000"
ALERTMANAGER_URL = "http://localhost:59093"
KAFKA_EXPORTER_URL = "http://localhost:9308"
GRAFANA_USER = "admin"
GRAFANA_PASS = "admin"

#Helpers functions 

def wait_for_http(url:str,timeout:float = 60.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(3)
    return False
 
 
def prom_query(expr: str) -> dict:
    """Run an instant query against Prometheus."""
    r = requests.get(
        f"{PROMETHEUS_URL}/api/v1/query",
        params={"query": expr},
        timeout=10,
    )
    assert r.status_code == 200, f"Prometheus query failed: {r.text}"
    return r.json()
 
 
def grafana_get(path: str) -> requests.Response:
    """Make an authenticated GET request to Grafana API."""
    return requests.get(
        f"{GRAFANA_URL}{path}",
        auth=(GRAFANA_USER, GRAFANA_PASS),
        timeout=10,
    )

#Prometheus tests
class TestPrometheusConfig:
 
    def test_prometheus_is_healthy(self):
        assert wait_for_http(f"{PROMETHEUS_URL}/-/healthy"), \
            "Prometheus /-/healthy did not return 200"
 
    def test_prometheus_is_ready(self):
        assert wait_for_http(f"{PROMETHEUS_URL}/-/ready"), \
            "Prometheus /-/ready did not return 200"
 
    def test_prometheus_config_is_valid(self):
        """Prometheus should report its config loaded without errors."""
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/status/config", timeout=10)
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "success", "Prometheus config status is not success"
        assert "scrape_configs" in data["data"]["yaml"], \
            "No scrape_configs found in loaded Prometheus config"
 
    def test_prometheus_kafka_scrape_job_exists(self):
        """Prometheus config should have the kafka scrape job."""
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/status/config", timeout=10)
        config_yaml = r.json()["data"]["yaml"]
        assert "kafka" in config_yaml, \
            "kafka scrape job not found in Prometheus config"
 
    def test_prometheus_has_active_targets(self):
        """Prometheus should have at least one active scrape target."""
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/targets", timeout=10)
        assert r.status_code == 200
        targets = r.json()["data"]["activeTargets"]
        assert len(targets) > 0, "No active scrape targets in Prometheus"
 
    def test_prometheus_kafka_target_is_up(self):
        """kafka-exporter scrape target should be UP."""
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/targets", timeout=10)
        targets = r.json()["data"]["activeTargets"]
        kafka_targets = [t for t in targets if t["labels"].get("job") == "kafka"]
        assert len(kafka_targets) > 0, \
            "No kafka scrape target found — check prometheus.yml job_name"
        up_targets = [t for t in kafka_targets if t["health"] == "up"]
        assert len(up_targets) > 0, \
            f"Kafka target is not UP. Health: {kafka_targets[0]['health']}. " \
            f"Last error: {kafka_targets[0].get('lastError', 'none')}"
 
    def test_prometheus_scrape_duration_is_reasonable(self):
        """Kafka scrape should complete within the 10s timeout."""
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/targets", timeout=10)
        targets = r.json()["data"]["activeTargets"]
        kafka_targets = [t for t in targets if t["labels"].get("job") == "kafka"]
        if kafka_targets:
            duration = kafka_targets[0].get("lastScrapeDuration", 0)
            assert duration < 10, \
                f"Kafka scrape took {duration:.2f}s — exceeds 10s timeout"
            
#Prometheus alert rules tests
class TestPrometheusAlertRules:
 
    def test_alert_rules_are_loaded(self):
        """Prometheus should have loaded alert rules from rules/*.yml."""
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/rules", timeout=10)
        assert r.status_code == 200
        groups = r.json()["data"]["groups"]
        assert len(groups) > 0, \
            "No alert rule groups loaded — check rule_files in prometheus.yml"
 
    def test_kafka_alert_rules_loaded(self):
        """kafka_alerts group should be loaded."""
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/rules", timeout=10)
        groups = r.json()["data"]["groups"]
        group_names = [g["name"] for g in groups]
        assert "kafka_alerts" in group_names, \
            f"kafka_alerts group not found. Loaded groups: {group_names}"
 
    def test_kafka_broker_down_alert_exists(self):
        """KafkaBrokerDown alert rule should be present."""
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/rules", timeout=10)
        groups = r.json()["data"]["groups"]
        all_rules = [rule for g in groups for rule in g["rules"]]
        rule_names = [r["name"] for r in all_rules if r.get("type") == "alerting"]
        assert "KafkaBrokerDown" in rule_names, \
            f"KafkaBrokerDown alert not found. Rules: {rule_names}"
 
    def test_kafka_consumer_lag_alert_exists(self):
        """KafkaConsumerLagHigh alert rule should be present."""
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/rules", timeout=10)
        groups = r.json()["data"]["groups"]
        all_rules = [rule for g in groups for rule in g["rules"]]
        rule_names = [r["name"] for r in all_rules if r.get("type") == "alerting"]
        assert "KafkaConsumerLagHigh" in rule_names, \
            f"KafkaConsumerLagHigh alert not found. Rules: {rule_names}"
 
    def test_all_expected_alerts_exist(self):
        """All expected alert rules should be loaded."""
        expected = [
            "KafkaBrokerDown",
            "KafkaConsumerLagHigh",
            "KafkaConsumerLagCritical",
            "KafkaTopicNoActivePartitions",
            "KafkaNoMessagesProduced",
        ]
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/rules", timeout=10)
        groups = r.json()["data"]["groups"]
        all_rules = [rule for g in groups for rule in g["rules"]]
        loaded = [r["name"] for r in all_rules if r.get("type") == "alerting"]
        missing = [a for a in expected if a not in loaded]
        assert not missing, f"Missing alert rules: {missing}"
 
    def test_no_alert_rules_have_errors(self):
        """All loaded alert rules should have valid expressions."""
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/rules", timeout=10)
        groups = r.json()["data"]["groups"]
        errors = []
        for group in groups:
            for rule in group["rules"]:
                if rule.get("lastError"):
                    errors.append(f"{rule['name']}: {rule['lastError']}")
        assert not errors, f"Alert rules with errors:\n" + "\n".join(errors)
 
#promethesu metrics tests
class TestPrometheusMetrics:
 
    def test_kafka_brokers_metric_exists(self):
        """kafka_brokers metric should be available."""
        result = prom_query("kafka_brokers")
        assert result["status"] == "success"
        assert len(result["data"]["result"]) > 0, \
            "kafka_brokers metric has no data — kafka-exporter may not be connected"
 
    def test_kafka_brokers_count_is_positive(self):
        """At least one Kafka broker should be reported as up."""
        result = prom_query("kafka_brokers")
        assert result["status"] == "success"
        values = [float(r["value"][1]) for r in result["data"]["result"]]
        assert any(v > 0 for v in values), \
            f"kafka_brokers value is 0 or negative: {values}"
 
    def test_kafka_topic_partitions_metric_exists(self):
        """kafka_topic_partitions metric should be available."""
        result = prom_query("kafka_topic_partitions")
        assert result["status"] == "success"
        assert len(result["data"]["result"]) > 0, \
            "kafka_topic_partitions metric has no data"
 
    def test_raw_topics_have_partitions(self):
        """All 5 raw topics should have at least 1 partition."""
        raw_topics = ["visa_raw", "mastercard_raw", "mpesa_raw", "pesapal_raw", "flutter_raw"]
        result = prom_query('kafka_topic_partitions{topic=~".*_raw"}')
        assert result["status"] == "success"
        found_topics = [r["metric"]["topic"] for r in result["data"]["result"]]
        missing = [t for t in raw_topics if t not in found_topics]
        assert not missing, f"Raw topics not found in Prometheus: {missing}"
 
    def test_kafka_consumer_lag_metric_exists(self):
        """kafka_consumergroup_lag metric should be queryable."""
        result = prom_query("kafka_consumergroup_lag")
        assert result["status"] == "success", \
            "kafka_consumergroup_lag query failed"
 
    def test_prometheus_self_scrape_metric(self):
        """Prometheus should be scraping itself (up metric for prometheus job)."""
        result = prom_query('up{job="kafka"}')
        assert result["status"] == "success"
        values = [float(r["value"][1]) for r in result["data"]["result"]]
        assert any(v == 1.0 for v in values), \
            "Kafka scrape target is not UP"
        
#Grafana config tests
class TestGrafanaConfig:
 
    def test_grafana_is_healthy(self):
        """Grafana /api/health should return 200 with database ok."""
        assert wait_for_http(f"{GRAFANA_URL}/api/health"), \
            "Grafana /api/health did not return 200"
        r = requests.get(f"{GRAFANA_URL}/api/health", timeout=10)
        data = r.json()
        assert data.get("database") == "ok", \
            f"Grafana database not ok: {data}"
 
    def test_grafana_prometheus_datasource_exists(self):
        """Prometheus datasource should be provisioned in Grafana."""
        r = grafana_get("/api/datasources")
        assert r.status_code == 200, \
            f"Grafana datasources API failed — check admin credentials ({r.status_code})"
        datasources = r.json()
        ds_types = [ds["type"] for ds in datasources]
        assert "prometheus" in ds_types, \
            f"No Prometheus datasource found. Datasources: {ds_types}"
 
    def test_grafana_prometheus_datasource_is_default(self):
        """Prometheus datasource should be set as default."""
        r = grafana_get("/api/datasources")
        datasources = r.json()
        prometheus_ds = [ds for ds in datasources if ds["type"] == "prometheus"]
        assert len(prometheus_ds) > 0, "No Prometheus datasource found"
        assert any(ds["isDefault"] for ds in prometheus_ds), \
            "Prometheus datasource is not set as default"
 
    def test_grafana_prometheus_datasource_url(self):
        """Prometheus datasource URL should point to prometheus:9090."""
        r = grafana_get("/api/datasources")
        datasources = r.json()
        prometheus_ds = [ds for ds in datasources if ds["type"] == "prometheus"]
        assert len(prometheus_ds) > 0
        url = prometheus_ds[0]["url"]
        assert "prometheus" in url and "9090" in url, \
            f"Prometheus datasource URL looks wrong: {url}"
 
    def test_grafana_prometheus_datasource_is_reachable(self):
        """Grafana should be able to reach the Prometheus datasource."""
        r = grafana_get("/api/datasources")
        datasources = r.json()
        prometheus_ds = [ds for ds in datasources if ds["type"] == "prometheus"]
        assert len(prometheus_ds) > 0
        ds_id = prometheus_ds[0]["id"]
        r = grafana_get(f"/api/datasources/uid/{prometheus_ds[0]['uid']}/health")
        assert r.status_code == 200, \
            f"Grafana cannot reach Prometheus datasource: {r.text}"
        data = r.json()
        assert data.get("status") == "OK", \
            f"Prometheus datasource health check failed: {data}"
        
#grafana dashboards tests
class TestGrafanaDashboards:
 
    def test_dashboards_are_provisioned(self):
        """At least one dashboard should be provisioned in Grafana."""
        r = grafana_get("/api/search?type=dash-db")
        assert r.status_code == 200
        dashboards = r.json()
        assert len(dashboards) > 0, \
            "No dashboards found in Grafana — check provisioning/dashboards/dashboards.yml"
 
    def test_kafka_dashboard_exists(self):
        """Kafka fraud detection dashboard should be provisioned."""
        r = grafana_get("/api/search?query=kafka")
        assert r.status_code == 200
        results = r.json()
        assert len(results) > 0, \
            "Kafka dashboard not found in Grafana — check kafka_dashboard.json"
 
    def test_containers_dashboard_exists(self):
        """Container resources dashboard should be provisioned."""
        r = grafana_get("/api/search?query=container")
        assert r.status_code == 200
        results = r.json()
        assert len(results) > 0, \
            "Container dashboard not found in Grafana — check containers_dashboard.json"
 
    def test_postgres_dashboard_exists(self):
        """Postgres dashboard should be provisioned."""
        r = grafana_get("/api/search?query=postgres")
        assert r.status_code == 200
        results = r.json()
        assert len(results) > 0, \
            "Postgres dashboard not found in Grafana — check postgres_dashboard.json"
 
    def test_kafka_dashboard_has_correct_uid(self):
        """Kafka dashboard should have the expected UID."""
        r = grafana_get("/api/dashboards/uid/kafka-fraud-pipeline")
        assert r.status_code == 200, \
            f"Kafka dashboard UID 'kafka-fraud-pipeline' not found: {r.status_code}"
        data = r.json()
        assert data["dashboard"]["title"] == "Kafka - Fraud Detection Pipeline", \
            f"Unexpected dashboard title: {data['dashboard']['title']}"
 
    def test_dashboards_use_prometheus_datasource(self):
        """All provisioned dashboards should reference the Prometheus datasource."""
        r = grafana_get("/api/search?type=dash-db")
        dashboards = r.json()
        for db in dashboards:
            detail = grafana_get(f"/api/dashboards/uid/{db['uid']}")
            if detail.status_code != 200:
                continue
            dashboard_json = str(detail.json())
            assert "expr" in dashboard_json, \
                f"Dashboard '{db['title']}' does not reference Prometheus datasource"
#grafana folder tests 
class TestGrafanaFolders:
 
    def test_fraud_detection_folder_exists(self):
        """Fraud Detection folder should be created by provisioning."""
        r = grafana_get("/api/folders")
        assert r.status_code == 200
        folders = r.json()
        folder_titles = [f["title"] for f in folders]
        assert "Fraud Detection" in folder_titles, \
            f"'Fraud Detection' folder not found. Folders: {folder_titles}"
 
