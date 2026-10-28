#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Disposable CAPD tenant with two external, mutually authenticated etcd quorums."""

import argparse
import base64
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
STATE = ROOT / "controller/.local/external-etcd"
NAME = "nvke-etcd-lab"
MGMT = NAME + "-mgmt"
NS = "external-etcd-lab"
LABEL = "nvke.external-etcd-lab=" + NAME
NODE_IMAGE = "kindest/node:v1.34.8"
ETCD_IMAGE = "registry.k8s.io/etcd:3.6.4-0@sha256:e36c081683425b5b3bc1425bc508b37e7107bb65dfa9367bf5a80125d431fa19"
MANAGEMENT = STATE / "management.kubeconfig"
TENANT = STATE / "tenant.kubeconfig"


def run(*args, data=None, capture=False):
    return subprocess.run([str(a) for a in args], input=data, text=True,
                          check=True, stdout=subprocess.PIPE if capture else None).stdout


def mgmt(*args, **kwargs):
    return run("kubectl", "--kubeconfig", MANAGEMENT, *args, **kwargs)


def tenant(*args, **kwargs):
    return run("kubectl", "--kubeconfig", TENANT, *args, **kwargs)


def apply(objects, target=mgmt):
    target("apply", "-f", "-", data=json.dumps({"apiVersion": "v1", "kind": "List", "items": objects}))


def wait_for(description, fn, seconds=600):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if fn():
                print("Ready: " + description, flush=True)
                return
        except (subprocess.CalledProcessError, KeyError, ValueError):
            pass
        time.sleep(5)
    raise RuntimeError("Timed out: " + description + "; run lab.py status and inspect the retained lab")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def clusterctl():
    candidates = [os.environ.get("CLUSTERCTL"), STATE / "tools/clusterctl",
                  ROOT / "controller/.local/bin/clusterctl", shutil.which("clusterctl")]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            version = run(candidate, "version", "-o", "short", capture=True).strip()
            pinned = next(line.split("?=")[1].strip() for line in
                          (ROOT / "make/tool-versions.mk").read_text().splitlines()
                          if line.startswith("CAPI_VERSION "))
            if version == pinned:
                return str(candidate), pinned
    raise RuntimeError("Install the pinned CLI: make -C controller clusterctl-install")


def certificate(directory, common_name, dns=None):
    directory.mkdir(parents=True, exist_ok=True)
    ca = STATE / "pki"
    extension = directory / "extensions.cnf"
    extension.write_text("extendedKeyUsage=serverAuth,clientAuth\n" +
                         ("subjectAltName=DNS:" + dns + "\n" if dns else ""))
    run("openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes",
        "-subj", "/CN=" + common_name, "-keyout", directory / "tls.key",
        "-out", directory / "tls.csr", capture=True)
    run("openssl", "x509", "-req", "-in", directory / "tls.csr",
        "-CA", ca / "ca.crt", "-CAkey", ca / "ca.key", "-CAcreateserial",
        "-days", "30", "-extfile", extension, "-out", directory / "tls.crt", capture=True)
    shutil.copyfile(ca / "ca.crt", directory / "ca.crt")


def member_name(shard, number):
    return f"{NAME}-{shard}-{number}"


def etcd(shard, *args, capture=True):
    return run("docker", "exec", member_name(shard, 1), "etcdctl",
               "--endpoints=" + ",".join(f"https://{member_name(shard, n)}:2379" for n in range(1, 4)),
               "--cacert=/pki/ca.crt", "--cert=/pki/tls.crt", "--key=/pki/tls.key",
               *args, capture=capture)


def create_stores():
    ca = STATE / "pki"
    if not (ca / "ca.crt").exists():
        ca.mkdir(parents=True)
        run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "30",
            "-subj", "/CN=nvke-external-etcd-lab", "-keyout", ca / "ca.key",
            "-out", ca / "ca.crt", capture=True)
        certificate(ca / "client", NAME + "-apiserver")
    for shard in ("default", "events"):
        peers = ",".join(f"{member_name(shard, n)}=https://{member_name(shard, n)}:2380" for n in range(1, 4))
        for n in range(1, 4):
            name = member_name(shard, n)
            pki = ca / name
            if not pki.exists():
                certificate(pki, NAME + "-" + shard, name)
            if name in run("docker", "ps", "-a", "--format", "{{.Names}}", capture=True).splitlines():
                owner = json.loads(run("docker", "inspect", name, capture=True))[0]["Config"]["Labels"].get("nvke.external-etcd-lab")
                if owner != NAME:
                    raise RuntimeError("Refusing to adopt an unowned container: " + name)
                continue
            volume = name + "-data"
            existing = run("docker", "volume", "ls", "--format", "{{.Name}}", capture=True).splitlines()
            if volume in existing:
                owner = json.loads(run("docker", "volume", "inspect", volume, capture=True))[0]["Labels"].get("nvke.external-etcd-lab")
                if owner != NAME:
                    raise RuntimeError("Refusing to adopt an unowned volume: " + volume)
            run("docker", "volume", "create", "--label", LABEL, volume, capture=True)
            run("docker", "run", "-d", "--name", name, "--label", LABEL, "--network", "kind",
                "--mount", f"type=bind,src={pki},dst=/pki,readonly",
                "--mount", f"type=volume,src={volume},dst=/data", ETCD_IMAGE,
                "etcd", "--name=" + name, "--data-dir=/data",
                "--initial-cluster=" + peers, "--initial-cluster-token=" + NAME + "-" + shard,
                "--initial-cluster-state=new", "--listen-client-urls=https://0.0.0.0:2379",
                "--advertise-client-urls=https://" + name + ":2379",
                "--listen-peer-urls=https://0.0.0.0:2380",
                "--initial-advertise-peer-urls=https://" + name + ":2380",
                "--cert-file=/pki/tls.crt", "--key-file=/pki/tls.key",
                "--trusted-ca-file=/pki/ca.crt", "--client-cert-auth=true",
                "--peer-cert-file=/pki/tls.crt", "--peer-key-file=/pki/tls.key",
                "--peer-trusted-ca-file=/pki/ca.crt", "--peer-client-cert-auth=true",
                "--peer-cert-allowed-cn=" + NAME + "-" + shard, capture=True)
    for shard in ("default", "events"):
        wait_for(shard + " etcd quorum", lambda s=shard: bool(etcd(s, "endpoint", "health")))


def resource(group, kind, name, spec):
    return {"apiVersion": group, "kind": kind, "metadata": {"name": name, "namespace": NS}, "spec": spec}


def manifests():
    core = "cluster.x-k8s.io/v1beta1"
    infra = "infrastructure.cluster.x-k8s.io/v1beta1"
    boot = "bootstrap.cluster.x-k8s.io/v1beta1"
    cp = "controlplane.cluster.x-k8s.io/v1beta1"
    def ref(api, kind, name):
        return {"apiVersion": api, "kind": kind, "name": name}
    labels = {"cluster.x-k8s.io/cluster-name": NAME}
    secrets = []
    for suffix, directory in [("etcd", STATE / "pki"), ("apiserver-etcd-client", STATE / "pki/client")]:
        files = {"tls.crt": directory / ("ca.crt" if suffix == "etcd" else "tls.crt")}
        if suffix != "etcd":
            files["tls.key"] = directory / "tls.key"
        secrets.append({"apiVersion": "v1", "kind": "Secret", "metadata": {
            "name": NAME + "-" + suffix, "namespace": NS, "labels": labels},
            "type": "cluster.x-k8s.io/secret",
            "data": {k: base64.b64encode(v.read_bytes()).decode() for k, v in files.items()}})
    node_registration = {"kubeletExtraArgs": {"eviction-hard": "nodefs.available<0%,nodefs.inodesFree<0%,imagefs.available<0%"}}
    configuration = {"clusterConfiguration": {
        "apiServer": {"certSANs": ["localhost", "127.0.0.1", "host.docker.internal"], "extraArgs": {
            "etcd-servers-overrides": "/events#" + ";".join(f"https://{member_name('events', n)}:2379" for n in range(1, 4))}},
        "etcd": {"external": {"endpoints": [f"https://{member_name('default', n)}:2379" for n in range(1, 4)],
            "caFile": "/etc/kubernetes/pki/etcd/ca.crt",
            "certFile": "/etc/kubernetes/pki/apiserver-etcd-client.crt",
            "keyFile": "/etc/kubernetes/pki/apiserver-etcd-client.key"}}},
        "initConfiguration": {"nodeRegistration": node_registration},
        "joinConfiguration": {"nodeRegistration": node_registration}}
    return [{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}}, *secrets,
        resource(infra, "DockerCluster", NAME, {}),
        resource(infra, "DockerMachineTemplate", NAME + "-nodes", {"template": {"spec": {"customImage": NODE_IMAGE}}}),
        resource(cp, "KubeadmControlPlane", NAME + "-cp", {"replicas": 3, "version": "v1.34.8",
            "machineTemplate": {"infrastructureRef": ref(infra, "DockerMachineTemplate", NAME + "-nodes")},
            "kubeadmConfigSpec": configuration}),
        resource(boot, "KubeadmConfigTemplate", NAME + "-workers", {"template": {"spec": {
            "joinConfiguration": {"nodeRegistration": node_registration}}}}),
        resource(core, "MachineDeployment", NAME + "-workers", {"clusterName": NAME, "replicas": 2,
            "selector": {"matchLabels": {"lab-pool": NAME}}, "template": {"metadata": {"labels": {"lab-pool": NAME}},
                "spec": {"clusterName": NAME, "version": "v1.34.8",
                    "bootstrap": {"configRef": ref(boot, "KubeadmConfigTemplate", NAME + "-workers")},
                    "infrastructureRef": ref(infra, "DockerMachineTemplate", NAME + "-nodes")}}}),
        resource(core, "Cluster", NAME, {"clusterNetwork": {"pods": {"cidrBlocks": ["192.168.0.0/16"]}},
            "infrastructureRef": ref(infra, "DockerCluster", NAME),
            "controlPlaneRef": ref(cp, "KubeadmControlPlane", NAME + "-cp")})]


def up():
    ctl, version = clusterctl()
    run("docker", "info", "--format", "Docker: {{.OSType}}/{{.Architecture}}")
    clusters = run("kind", "get", "clusters", capture=True).splitlines()
    if MGMT not in clusters:
        config = {"kind": "Cluster", "apiVersion": "kind.x-k8s.io/v1alpha4", "nodes": [{"role": "control-plane",
            "extraMounts": [{"hostPath": "/var/run/docker.sock", "containerPath": "/var/run/docker.sock"}]}]}
        run("kind", "create", "cluster", "--name", MGMT, "--image", NODE_IMAGE,
            "--kubeconfig", MANAGEMENT, "--config", "-", "--wait", "180s", data=json.dumps(config))
    elif not MANAGEMENT.exists():
        raise RuntimeError("Management cluster exists without this lab's kubeconfig; refusing to adopt it")
    run(ctl, "init", "--kubeconfig", MANAGEMENT, "--core", "cluster-api:" + version,
        "--bootstrap", "kubeadm:" + version, "--control-plane", "kubeadm:" + version,
        "--infrastructure", "docker:" + version, "--wait-providers")
    create_stores()
    objects = manifests()
    (STATE / "resources.json").write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": objects}, indent=2))
    apply(objects)
    def kubeconfig():
        secret = json.loads(mgmt("-n", NS, "get", "secret", NAME + "-kubeconfig", "-o", "json", capture=True))
        TENANT.write_bytes(base64.b64decode(secret["data"]["value"]))
        return True
    wait_for("tenant kubeconfig", kubeconfig)
    # CAPD's load balancer publishes a host port; container-network IPs are not routable on macOS.
    port = run("docker", "inspect", NAME + "-lb", "--format",
               '{{(index (index .NetworkSettings.Ports "6443/tcp") 0).HostPort}}', capture=True).strip()
    run("kubectl", "--kubeconfig", TENANT, "config", "set-cluster", NAME, "--server=https://127.0.0.1:" + port)
    wait_for("tenant API", lambda: bool(tenant("get", "--raw=/readyz", capture=True)))
    # Reuse kind's versioned CNI image and manifests, adjusting the tenant Pod CIDR.
    cni = json.loads(mgmt("-n", "kube-system", "get", "daemonset", "kindnet", "-o", "json", capture=True))
    cni["metadata"] = {"name": "kindnet", "namespace": "kube-system"}
    cni.pop("status", None)
    endpoint = json.loads(mgmt("-n", NS, "get", "dockercluster", NAME, "-o", "json", capture=True))["spec"]["controlPlaneEndpoint"]
    for env in cni["spec"]["template"]["spec"]["containers"][0]["env"]:
        if env["name"] == "POD_SUBNET":
            env["value"] = "192.168.0.0/16"
        if env["name"] == "CONTROL_PLANE_ENDPOINT":
            env["value"] = endpoint["host"] + ":" + str(endpoint["port"])
    cni_role = json.loads(mgmt("get", "clusterrole", "kindnet", "-o", "json", capture=True))
    cni_role["metadata"] = {"name": "kindnet"}
    rbac = [{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "kindnet", "namespace": "kube-system"}},
        cni_role,
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": {"name": "kindnet"},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "kindnet"},
         "subjects": [{"kind": "ServiceAccount", "name": "kindnet", "namespace": "kube-system"}]}]
    apply([*rbac, cni], target=tenant)
    wait_for("five tenant nodes", lambda: len(json.loads(tenant("get", "nodes", "-o", "json", capture=True))["items"]) == 5)
    tenant("wait", "nodes", "--all", "--for=condition=Ready", "--timeout=600s")
    verify()


def verify():
    tenant("wait", "nodes", "--all", "--for=condition=Ready", "--timeout=120s")
    nodes = json.loads(tenant("get", "nodes", "-o", "json", capture=True))["items"]
    control_planes = [n["metadata"]["name"] for n in nodes
                      if "node-role.kubernetes.io/control-plane" in n["metadata"].get("labels", {})]
    require(len(control_planes) == 3 and len(nodes) == 5, "Expected three API hosts and two workers")
    for name in control_planes:
        run("docker", "exec", name, "test", "!", "-e", "/etc/kubernetes/manifests/etcd.yaml")
        manifest = run("docker", "exec", name, "cat", "/etc/kubernetes/manifests/kube-apiserver.yaml", capture=True)
        require("--etcd-servers-overrides=/events#" in manifest, "Missing Events storage override on " + name)
        require(all(member_name("default", n) in manifest and member_name("events", n) in manifest
                    for n in range(1, 4)), "Every API replica must use both complete endpoint lists")
        run("docker", "exec", name, "kubectl", "--kubeconfig=/etc/kubernetes/admin.conf",
            "--server=https://127.0.0.1:6443", "get", "--raw=/readyz", capture=True)
    tenant("get", "nodes", "-o", "wide")
    tenant("get", "--raw=/readyz")
    cluster_ids = []
    for shard in ("default", "events"):
        statuses = json.loads(etcd(shard, "endpoint", "status", "-w", "json"))
        require(len(statuses) == 3, "Expected three endpoints for " + shard)
        members = json.loads(etcd(shard, "member", "list", "-w", "json"))["members"]
        require(len(members) == 3 and not any(m.get("isLearner", False) for m in members), "Expected three voters for " + shard)
        ids = {s["Status"]["header"]["cluster_id"] for s in statuses}
        require(len(ids) == 1, "Endpoints disagree on cluster identity for " + shard)
        cluster_ids.append(ids.pop())
        print(f"{shard}: three endpoints, one healthy cluster", flush=True)
        etcd(shard, "endpoint", "health")
    require(len(set(cluster_ids)) == 2, "The two shards must be independent clusters")
    apply([{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "lab-smoke"}},
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "routing-proof", "namespace": "lab-smoke"}, "data": {"proof": "default-store"}},
        {"apiVersion": "v1", "kind": "Event", "metadata": {"name": "routing-proof", "namespace": "lab-smoke"},
         "involvedObject": {"apiVersion": "v1", "kind": "ConfigMap", "name": "routing-proof", "namespace": "lab-smoke"},
         "reason": "LabVerification", "message": "external events store", "type": "Normal"}], target=tenant)
    def keys(shard, prefix):
        return etcd(shard, "get", prefix, "--prefix", "--keys-only")
    require("routing-proof" in keys("default", "/registry/configmaps/lab-smoke/"), "ConfigMap absent from default store")
    require(not keys("events", "/registry/configmaps/lab-smoke/").strip(), "ConfigMap leaked into Events store")
    require("routing-proof" in keys("events", "/registry/events/lab-smoke/"), "Event absent from Events store")
    require(not keys("default", "/registry/events/lab-smoke/").strip(), "Event leaked into default store")
    core_event = json.loads(tenant("get", "--raw=/api/v1/namespaces/lab-smoke/events/routing-proof", capture=True))
    new_event = json.loads(tenant("get", "--raw=/apis/events.k8s.io/v1/namespaces/lab-smoke/events/routing-proof", capture=True))
    require(core_event["metadata"]["uid"] == new_event["metadata"]["uid"], "Event API aliases disagree on storage")
    deployment = json.loads(tenant("-n", "lab-smoke", "create", "deployment", "web", "--image=nginx:alpine", "--replicas=2", "--dry-run=client", "-o", "json", capture=True))
    deployment["spec"]["template"]["spec"]["affinity"] = {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
        "labelSelector": {"matchLabels": {"app": "web"}}, "topologyKey": "kubernetes.io/hostname"}]}}
    service = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "web", "namespace": "lab-smoke"},
               "spec": {"selector": {"app": "web"}, "ports": [{"port": 80, "targetPort": 80}]}}
    apply([deployment, service], target=tenant)
    tenant("-n", "lab-smoke", "rollout", "status", "deployment/web", "--timeout=300s")
    tenant("-n", "lab-smoke", "exec", "deployment/web", "--", "sh", "-c", "wget -qO- http://127.0.0.1 >/dev/null && getent hosts kubernetes.default.svc.cluster.local")
    pods = json.loads(tenant("-n", "lab-smoke", "get", "pods", "-l", "app=web", "-o", "json", capture=True))["items"]
    require(len({p["spec"]["nodeName"] for p in pods}) == 2, "Workload replicas must span two workers")
    require(all(p["spec"]["nodeName"] not in control_planes for p in pods), "Workloads must run on workers")
    for source in pods:
        for destination in pods:
            tenant("-n", "lab-smoke", "exec", source["metadata"]["name"], "--", "wget", "-qO", "/dev/null",
                   "http://" + destination["status"]["podIP"])
        tenant("-n", "lab-smoke", "exec", source["metadata"]["name"], "--", "wget", "-qO", "/dev/null", "http://web.lab-smoke.svc.cluster.local")
    print("PASS: independent mTLS quorums, default/Event routing, workload readiness, cross-worker HTTP, Service and DNS")
    print("Tenant access: kubectl --kubeconfig " + str(TENANT) + " get nodes")


def down():
    if MANAGEMENT.exists():
        mgmt("-n", NS, "delete", "cluster", NAME, "--ignore-not-found", "--wait=true", "--timeout=300s")
        run("kind", "delete", "cluster", "--name", MGMT, "--kubeconfig", MANAGEMENT)
    for kind, field in [("container", "{{.ID}}"), ("volume", "{{.Name}}")]:
        ids = run("docker", kind, "ls", "--filter", "label=" + LABEL, "--format", field,
                  *( ["-a"] if kind == "container" else []), capture=True).splitlines()
        for identifier in ids:
            run("docker", kind, "rm", "-f", identifier)
    # Remove credentials after deleting the lab's data; keep the tool cache and logs.
    for path in [STATE / "pki", STATE / "resources.json", MANAGEMENT, TENANT]:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)
    (STATE / "docker-context").unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["up", "verify", "status", "down"])
    action = parser.parse_args().action
    os.umask(0o077)
    STATE.mkdir(parents=True, exist_ok=True)
    current_context = run("docker", "context", "show", capture=True).strip()
    context_file = STATE / "docker-context"
    if context_file.exists() and context_file.read_text() != current_context:
        raise RuntimeError("Docker context differs from the one that owns this lab")
    if not context_file.exists():
        if action != "up":
            raise RuntimeError("No lab ownership state; run up first")
        context_file.write_text(current_context)
    if action == "status":
        mgmt("-n", NS, "get", "clusters,kubeadmcontrolplanes,machines")
        run("docker", "ps", "-a", "--filter", "label=" + LABEL)
    else:
        {"up": up, "verify": verify, "down": down}[action]()


if __name__ == "__main__":
    try:
        main()
    except (subprocess.CalledProcessError, RuntimeError) as error:
        sys.exit(str(error))
