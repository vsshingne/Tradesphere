# TradeSphere — Production GCP & GKE Deployment Guide

This guide provides exhaustive, step-by-step instructions for deploying the **TradeSphere Platform** to Google Kubernetes Engine (GKE) using Google Artifact Registry (GAR), Helm / Kustomize, and Google Cloud Observability.

---

## 📋 Table of Contents
1. [Prerequisites & GCP CLI Setup](#1-prerequisites--gcp-cli-setup)
2. [GCP Project & API Configuration](#2-gcp-project--api-configuration)
3. [GCP Artifact Registry Repository Setup](#3-gcp-artifact-registry-repository-setup)
4. [GKE Cluster Provisioning](#4-gke-cluster-provisioning)
5. [Workload Identity & IAM Binding](#5-workload-identity--iam-binding)
6. [Secret Management](#6-secret-management)
7. [Container Image Build & Push](#7-container-image-build--push)
8. [Helm Deployment](#8-helm-deployment)
9. [Raw Kustomize Deployment Alternative](#9-raw-kustomize-deployment-alternative)
10. [Ingress, Static IP & SSL Configuration](#10-ingress-static-ip--ssl-configuration)
11. [Observability & Dashboard Verification](#11-observability--dashboard-verification)
12. [Post-Deployment Health & Sanity Verification](#12-post-deployment-health--sanity-verification)
13. [Teardown & Cleanup](#13-teardown--cleanup)

---

## 1. Prerequisites & GCP CLI Setup

Ensure you have the following CLI utilities installed on your administrative workstation:
- **Google Cloud SDK (`gcloud`)**: `>= 450.0.0`
- **kubectl**: `>= 1.28.0`
- **Helm**: `>= v3.12.0`
- **Docker**: `>= 24.0.0`

```bash
# Authenticate gcloud CLI
gcloud auth login
gcloud auth application-default login

# Set your target project ID and region
export GCP_PROJECT_ID="your-gcp-project-id"
export GCP_REGION="us-central1"
export GCP_ZONE="us-central1-a"
export GKE_CLUSTER_NAME="tradesphere-prod"
export GAR_REPO_NAME="tradesphere"
export K8S_NAMESPACE="tradesphere"

gcloud config set project ${GCP_PROJECT_ID}
gcloud config set compute/region ${GCP_REGION}
gcloud config set compute/zone ${GCP_ZONE}
```

---

## 2. GCP Project & API Configuration

Enable the required Google Cloud API services:

```bash
gcloud services enable \
  container.googleapis.com \
  artifactregistry.googleapis.com \
  iam.googleapis.com \
  secretmanager.googleapis.com \
  compute.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com
```

---

## 3. GCP Artifact Registry Repository Setup

Create a Docker repository in GCP Artifact Registry:

```bash
# Create repository
gcloud artifacts repositories create ${GAR_REPO_NAME} \
  --repository-format=docker \
  --location=${GCP_REGION} \
  --description="TradeSphere Production Container Images"

# Configure Docker CLI authentication for Artifact Registry
gcloud auth configure-docker ${GCP_REGION}-docker.pkg.dev --quiet
```

Target Registry URL format:
`${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${GAR_REPO_NAME}`

---

## 4. GKE Cluster Provisioning

Provision a production-grade, highly-available regional GKE cluster with Workload Identity enabled:

```bash
# Create regional GKE cluster
gcloud container clusters create ${GKE_CLUSTER_NAME} \
  --region=${GCP_REGION} \
  --release-channel=regular \
  --workload-pool=${GCP_PROJECT_ID}.svc.id.goog \
  --enable-autoscaling \
  --min-nodes=1 \
  --max-nodes=5 \
  --num-nodes=2 \
  --machine-type=e2-standard-4 \
  --disk-size=50GB \
  --disk-type=pd-ssd \
  --enable-ip-alias \
  --shielded-nodes \
  --enable-vertical-pod-autoscaling

# Get cluster credentials for kubectl
gcloud container clusters get-credentials ${GKE_CLUSTER_NAME} --region=${GCP_REGION}

# Create dedicated namespace
kubectl create namespace ${K8S_NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -
```

---

## 5. Workload Identity & IAM Binding

Link Kubernetes Service Accounts (KSA) to Google Service Accounts (GSA) for secure, keyless access:

```bash
# Create GCP Service Account
gcloud iam service-accounts create tradesphere-sa \
  --display-name="TradeSphere GKE Service Account"

# Grant roles to GCP SA (Secret Manager Access + Logging)
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} \
  --member="serviceAccount:tradesphere-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} \
  --member="serviceAccount:tradesphere-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/logging.logWriter"

# Annotate Kubernetes Service Account for Workload Identity
kubectl annotate serviceaccount default \
  --namespace ${K8S_NAMESPACE} \
  iam.gke.io/gcp-service-account=tradesphere-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com

# Allow KSA to impersonate GSA
gcloud iam service-accounts add-iam-policy-binding \
  tradesphere-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${GCP_PROJECT_ID}.svc.id.goog[${K8S_NAMESPACE}/default]"
```

---

## 6. Secret Management

Create strong production credentials and inject them into Kubernetes Secrets:

```bash
# Generate strong secrets
export PROD_POSTGRES_USER="tradesphere_prod"
export PROD_POSTGRES_DB="tradesphere"
export PROD_POSTGRES_PASS=$(openssl rand -base64 32)
export PROD_JWT_SECRET=$(openssl rand -hex 64)

# Create Kubernetes Secret
kubectl create secret generic tradesphere-secret \
  --namespace ${K8S_NAMESPACE} \
  --from-literal=POSTGRES_USER="${PROD_POSTGRES_USER}" \
  --from-literal=POSTGRES_PASSWORD="${PROD_POSTGRES_PASS}" \
  --from-literal=JWT_SECRET="${PROD_JWT_SECRET}" \
  --dry-run=client -o yaml | kubectl apply -f -

# (Optional) Store in GCP Secret Manager for backup
gcloud secrets create tradesphere-db-pass --replication-policy="automatic"
echo -n "${PROD_POSTGRES_PASS}" | gcloud secrets versions add tradesphere-db-pass --data-file=-

gcloud secrets create tradesphere-jwt-secret --replication-policy="automatic"
echo -n "${PROD_JWT_SECRET}" | gcloud secrets versions add tradesphere-jwt-secret --data-file=-
```

---

## 7. Container Image Build & Push

Build all 7 microservices from the repository root context and push them to Google Artifact Registry:

```bash
export REGISTRY="${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${GAR_REPO_NAME}"
export IMAGE_TAG="v1.0.0"

# Build & Push API Gateway
docker build -t ${REGISTRY}/tradesphere-api-gateway:${IMAGE_TAG} -f services/api-gateway/Dockerfile .
docker push ${REGISTRY}/tradesphere-api-gateway:${IMAGE_TAG}

# Build & Push User Service
docker build -t ${REGISTRY}/tradesphere-user-service:${IMAGE_TAG} -f services/user-service/Dockerfile .
docker push ${REGISTRY}/tradesphere-user-service:${IMAGE_TAG}

# Build & Push Order Service
docker build -t ${REGISTRY}/tradesphere-order-service:${IMAGE_TAG} -f services/order-service/Dockerfile .
docker push ${REGISTRY}/tradesphere-order-service:${IMAGE_TAG}

# Build & Push Portfolio Service
docker build -t ${REGISTRY}/tradesphere-portfolio-service:${IMAGE_TAG} -f services/portfolio-service/Dockerfile .
docker push ${REGISTRY}/tradesphere-portfolio-service:${IMAGE_TAG}

# Build & Push Matching Engine
docker build -t ${REGISTRY}/tradesphere-matching-engine:${IMAGE_TAG} -f services/matching-engine/Dockerfile .
docker push ${REGISTRY}/tradesphere-matching-engine:${IMAGE_TAG}

# Build & Push WebSocket Service
docker build -t ${REGISTRY}/tradesphere-websocket-service:${IMAGE_TAG} -f services/websocket-service/Dockerfile .
docker push ${REGISTRY}/tradesphere-websocket-service:${IMAGE_TAG}

# Build & Push Frontend
docker build -t ${REGISTRY}/tradesphere-frontend:${IMAGE_TAG} -f frontend/Dockerfile .
docker push ${REGISTRY}/tradesphere-frontend:${IMAGE_TAG}
```

---

## 8. Helm Deployment

Deploy the full TradeSphere application stack using Helm with custom image overrides:

```bash
# Validate chart syntax
helm lint infra/helm/tradesphere/

# Execute atomic installation/upgrade
helm upgrade --install tradesphere ./infra/helm/tradesphere \
  --namespace ${K8S_NAMESPACE} \
  --create-namespace \
  --wait \
  --timeout 10m \
  --atomic \
  --set apiGateway.image=${REGISTRY}/tradesphere-api-gateway:${IMAGE_TAG} \
  --set userService.image=${REGISTRY}/tradesphere-user-service:${IMAGE_TAG} \
  --set orderService.image=${REGISTRY}/tradesphere-order-service:${IMAGE_TAG} \
  --set portfolioService.image=${REGISTRY}/tradesphere-portfolio-service:${IMAGE_TAG} \
  --set matchingEngine.image=${REGISTRY}/tradesphere-matching-engine:${IMAGE_TAG} \
  --set websocketService.image=${REGISTRY}/tradesphere-websocket-service:${IMAGE_TAG} \
  --set frontend.image=${REGISTRY}/tradesphere-frontend:${IMAGE_TAG} \
  --set secrets.POSTGRES_USER="${PROD_POSTGRES_USER}" \
  --set secrets.POSTGRES_PASSWORD="${PROD_POSTGRES_PASS}" \
  --set secrets.JWT_SECRET="${PROD_JWT_SECRET}"
```

---

## 9. Raw Kustomize Deployment Alternative

If deploying without Helm, use the pre-built `k8s/` raw manifests with Kustomize:

```bash
# 1. Update image tags in k8s/services.yaml and k8s/api-gateway.yaml to point to ${REGISTRY}

# 2. Apply via Kustomize
kubectl apply -k k8s/
```

---

## 10. Ingress, Static IP & SSL Configuration

Provision a global static IP address in GCP and set up Ingress:

```bash
# Reserve global static external IP
gcloud compute addresses create tradesphere-ip --global

# Get reserved IP address
export STATIC_IP=$(gcloud compute addresses describe tradesphere-ip --global --format='value(address)')
echo "Reserved Static IP: ${STATIC_IP}"

# Install NGINX Ingress Controller (if using NGINX)
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
helm install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx --create-namespace \
  --set controller.service.loadBalancerIP=${STATIC_IP}
```

Point your DNS record `A tradesphere.yourdomain.com` to `${STATIC_IP}`.

---

## 11. Observability & Dashboard Verification

Access Prometheus metrics and Grafana dashboards locally via port-forwarding:

```bash
# Port-forward Grafana
kubectl port-forward svc/grafana 3000:3000 -n ${K8S_NAMESPACE} &

# Port-forward Prometheus
kubectl port-forward svc/prometheus 9090:9090 -n ${K8S_NAMESPACE} &
```

- **Grafana URL**: `http://localhost:3000` (User: `admin`, Password: `${PROD_POSTGRES_PASS}`)
- **Dashboards**: Pre-loaded in Grafana under folder `TradeSphere`:
  1. *TradeSphere — Trading Dashboard*
  2. *TradeSphere — Kafka Dashboard*
  3. *TradeSphere — Infrastructure Dashboard*
  4. *TradeSphere — Database Dashboard*

---

## 12. Post-Deployment Health & Sanity Verification

Verify all services, pods, Kafka topics, and database connectivity:

```bash
# 1. Verify Pod Status (All should be Running / Completed)
kubectl get pods -n ${K8S_NAMESPACE} -o wide

# 2. Check rollout status of all deployments
kubectl rollout status deployment/api-gateway -n ${K8S_NAMESPACE}
kubectl rollout status deployment/order-service -n ${K8S_NAMESPACE}
kubectl rollout status deployment/portfolio-service -n ${K8S_NAMESPACE}
kubectl rollout status deployment/user-service -n ${K8S_NAMESPACE}
kubectl rollout status deployment/matching-engine -n ${K8S_NAMESPACE}
kubectl rollout status deployment/websocket-service -n ${K8S_NAMESPACE}

# 3. Verify Kafka Topic Auto-Creation Job output
kubectl logs job/kafka-init -n ${K8S_NAMESPACE}

# 4. Test REST API via API Gateway
kubectl port-forward svc/api-gateway 8000:8000 -n ${K8S_NAMESPACE} &
curl -i http://localhost:8000/healthz

# 5. Execute user signup test
curl -i -X POST http://localhost:8000/api/v1/auth/signup \
  -H "Content-Type: application/json" \
  -d '{"email":"gke_test@example.com","password":"password123"}'
```

---

## 13. Teardown & Cleanup

To destroy all cloud resources and prevent ongoing billing:

```bash
# 1. Uninstall Helm release
helm uninstall tradesphere --namespace ${K8S_NAMESPACE}

# 2. Delete static IP
gcloud compute addresses delete tradesphere-ip --global --quiet

# 3. Delete GKE Cluster
gcloud container clusters delete ${GKE_CLUSTER_NAME} --region=${GCP_REGION} --quiet

# 4. Delete Artifact Registry Repository
gcloud artifacts repositories delete ${GAR_REPO_NAME} --location=${GCP_REGION} --quiet
```
