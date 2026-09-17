# k8s semi-project

Kafka 기반 CQRS 예제를 kubeadm 온프레미스 클러스터에 올린 구성이다.
producer 가 이벤트를 Kafka 로 쓰고, consumer 가 그것을 읽어 MongoDB 에 조회용 모델을
만든다. MySQL 은 producer 쪽 쓰기 저장소다.

이 문서는 **클러스터를 처음부터 다시 세울 때의 순서**를 기록한 것이다.
매니페스트만 apply 하면 되는 부분과, 손으로 먼저 해줘야 하는 부분(노드 라벨, NFS,
Kafka VM)이 섞여 있어서 순서를 틀리면 Pod 가 Pending 이나 CrashLoop 로 멈춘다.

## 구성

VM 4대. 모두 같은 서브넷(192.168.0.0/24)에 있고 공유기 DHCP 대역 밖의 주소를 쓴다.

| 호스트 | IP | 역할 |
|---|---|---|
| master | 192.168.0.101 | control-plane, kubectl 실행 지점 |
| worker1 | 192.168.0.102 | 워크로드 + monitoring |
| worker2 | 192.168.0.103 | 워크로드 + monitoring |
| worker3 | 192.168.0.104 | **클러스터 외부.** Kafka 브로커 + NFS 서버 |

worker3 은 k8s 노드가 아니다. Kafka 를 클러스터 안에 올리지 않고 VM 에 직접 띄운 뒤
Service + Endpoints 로 끌어다 쓰는 구조라서, `kubectl get nodes` 에는 안 보인다.

### 버전

| 컴포넌트 | 버전 |
|---|---|
| Kubernetes | v1.30.14 (kubeadm) |
| CNI | flannel v0.28.9 |
| MetalLB | v0.13.12 |
| ingress-nginx | v1.11.3 |
| kube-prometheus-stack | chart 90.0.0 (Grafana 13.2.1) |
| ArgoCD | v3.5.3 |
| MySQL / MongoDB | 8.0 / 7.0.40 |

### 디렉터리

```
ns/          네임스페이스 (semi-app, semi-db)
limitRange/  네임스페이스별 리소스 기본값/상한
pv/          PV + PVC (mongodb 로컬 2개, mysql 용 NFS 1개)
secret/      DB 인증정보, 앱이 참조하는 접속 문자열
db/          mysql / mongodb StatefulSet + headless Service
app/         producer / consumer Deployment + ClusterIP Service
kafka/       외부 Kafka 를 가리키는 Service + Endpoints
ingress/     Ingress + MetalLB IPAddressPool
argocd/      argocd-server NodePort 노출
monitoring-values.yaml   kube-prometheus-stack helm values
rollout.sh   Secret 변경 감지 후 무중단 롤링 업데이트
```

`ingress/` 에 MetalLB 설정이 같이 있는 건 의도된 것이다. ingress 의 EXTERNAL-IP
192.168.0.150 을 만들어주는 게 MetalLB 이고, 그게 없으면 ingress 규칙 전체가
외부에서 안 닿으므로 같은 선행 조건으로 묶어뒀다.

---

## 설치 순서

### 0. 전제

kubeadm 으로 클러스터가 올라가 있고 flannel 이 Ready 인 상태에서 시작한다.
worker3(192.168.0.104)에 Kafka 와 NFS 가 떠 있어야 한다.

```bash
# NFS 서버 (worker3) — mysql 이 이 경로를 RWX 로 마운트한다
sudo mkdir -p /nfs/k8s
sudo chmod 777 /nfs/k8s
echo "/nfs/k8s 192.168.0.0/24(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports
sudo exportfs -ra

# 모든 k8s 노드에 NFS 클라이언트가 필요하다. 없으면 PVC 는 Bound 인데
# Pod 가 mount 단계에서 실패한다 (ContainerCreating 에서 멈춤).
sudo apt-get install -y nfs-common

# Kafka (worker3) — 9092 를 0.0.0.0 으로 열어야 클러스터에서 붙는다.
# advertised.listeners 가 localhost 면 Pod 에서 접속이 끊긴다.
```

### 1. MetalLB

ingress-nginx 보다 **먼저** 올려야 한다. 순서가 바뀌면 ingress-nginx Service 가
EXTERNAL-IP 를 못 받고 `<pending>` 으로 남는다 (나중에 MetalLB 를 올리면 결국
배정되긴 하지만, 그 사이 ingress 가 안 되는 이유를 찾느라 시간을 쓴다).

```bash
kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.13.12/config/manifests/metallb-native.yaml
kubectl -n metallb-system rollout status deploy/controller --timeout=5m

# CRD 가 등록된 뒤에 apply 해야 한다. 위 rollout status 를 생략하면
# "no matches for kind IPAddressPool" 로 실패한다.
kubectl apply -f ingress/metallb-pool.yaml
```

### 2. ingress-nginx

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.11.3/deploy/static/provider/baremetal/deploy.yaml

# EXTERNAL-IP 가 192.168.0.150 으로 뜨는지 확인. <pending> 이면 1번을 다시 본다.
kubectl -n ingress-nginx get svc ingress-nginx-controller
```

baremetal provider 는 Service 를 NodePort 로 만든다. MetalLB 를 쓰려면
LoadBalancer 로 바꿔줘야 한다.

```bash
kubectl -n ingress-nginx patch svc ingress-nginx-controller \
  -p '{"spec":{"type":"LoadBalancer"}}'
```

### 3. 노드 라벨

monitoring-values.yaml 이 Grafana / Prometheus / Operator 를 전부
`nodeSelector: monitoring: "true"` 로 배치한다. 라벨이 없으면 Pod 가 Pending 에서
움직이지 않는다.

```bash
kubectl label node worker1 worker2 monitoring=true
kubectl get nodes -L monitoring   # worker1, worker2 에 true 확인
```

### 4. 모니터링 (kube-prometheus-stack)

helm 이 필요하다. 현재 master 에는 helm 바이너리가 없으니 먼저 설치한다.

```bash
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm install monitoring prometheus-community/kube-prometheus-stack \
  --version 90.0.0 \
  -n monitoring --create-namespace \
  -f monitoring-values.yaml
```

릴리스 이름은 `monitoring` 이어야 한다. Service 이름(`monitoring-grafana` 등)이
릴리스 이름에서 나오므로, 다른 이름으로 설치하면 아래 접속/조회 명령이 전부 어긋난다.

values 를 고친 뒤에는:

```bash
helm upgrade monitoring prometheus-community/kube-prometheus-stack \
  --version 90.0.0 -n monitoring -f monitoring-values.yaml
```

### 5. ArgoCD

```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v3.5.3/manifests/install.yaml
kubectl -n argocd rollout status deploy/argocd-server --timeout=10m

# install.yaml 이 ClusterIP 로 만든 Service 를 NodePort 로 덮어쓴다.
# install.yaml 을 다시 apply 할 때마다 이 줄도 다시 실행해야 한다.
kubectl apply -f argocd/argocd-server-nodeport.yaml
```

### 6. 애플리케이션

앞의 순서를 지켰다면 여기부터는 apply 순서만 맞추면 된다.
PV → 네임스페이스 → 상한 → Secret → DB → 앱 순이다.

```bash
kubectl apply -f ns/
kubectl apply -f limitRange/      # 앱보다 먼저. 나중에 걸면 기존 Pod 에 적용 안 됨
kubectl apply -f pv/
kubectl apply -f secret/
kubectl apply -f db/
kubectl -n semi-db rollout status sts/mysql --timeout=5m
kubectl -n semi-db rollout status sts/mongodb --timeout=5m

kubectl apply -f kafka/           # 앱이 부팅 때 kafka:9092 를 찾으므로 앱보다 먼저
kubectl apply -f app/
kubectl apply -f ingress/
```

`limitRange/` 를 앱보다 먼저 적용하는 이유: LimitRange 는 **생성 시점에** Pod 에
기본값을 주입한다. 이미 떠 있는 Pod 에는 소급 적용되지 않아서, 나중에 걸면
재시작할 때까지 상한 없이 도는 Pod 가 남는다.

---

## 접속

| 대상 | 주소 |
|---|---|
| producer | http://192.168.0.150/books (Swagger: `/swagger-ui.html`) |
| consumer | http://192.168.0.150/cqrs/book |
| Grafana | http://192.168.0.101:30030 |
| Prometheus | http://192.168.0.101:30090 |
| ArgoCD | https://192.168.0.101:30443 |

호스트명으로 붙고 싶으면 클라이언트 hosts 파일에 등록한다:

```
192.168.0.150 producer.semi-project.local
192.168.0.150 consumer.semi-project.local
```

NodePort 는 모든 노드에 열리므로 master IP 로 붙어도 kube-proxy 가
실제 Pod 가 있는 worker 로 넘겨준다.

ArgoCD 는 HTTPS 만 받는다. 자체 서명 인증서라 브라우저 경고를 한 번 넘겨야 한다.
자세한 이유는 [argocd/argocd-server-nodeport.yaml](argocd/argocd-server-nodeport.yaml) 주석에 적어뒀다.

### 비밀번호

둘 다 설치 시 자동 생성되는 값이라 이 문서에 적지 않는다. 설치할 때마다 바뀐다.

```bash
# Grafana (admin)
kubectl -n monitoring get secret monitoring-grafana \
  -o jsonpath='{.data.admin-password}' | base64 -d; echo

# ArgoCD (admin)
kubectl -n argocd get secret argocd-initial-admin-secret \
  -o jsonpath='{.data.password}' | base64 -d; echo
```

Grafana 비밀번호를 고정하려면 monitoring-values.yaml 의 `grafana:` 에
`adminPassword` 를 넣거나 `admin.existingSecret` 으로 별도 Secret 을 가리킨다.
지금은 지정하지 않았으므로 `helm upgrade` 때 바뀔 수 있다.

---

## 운영

Secret 을 고친 뒤에는 `rollout.sh` 를 쓴다.

```bash
./rollout.sh            # 변경분만 롤아웃
./rollout.sh --force    # 변경이 없어도 재시작
```

Deployment 는 Secret 의 **이름**만 참조하므로 내용이 바뀌어도 pod template 이
그대로여서 롤아웃이 안 일어난다. 이 스크립트가 내용 해시를 template annotation 에
넣어 template 자체를 바꿔주는 역할을 한다.

`rollout.sh` 가 다루는 건 `ns/`, `secret/`, `app/` 뿐이다. 나머지(`pv/`, `db/`,
`kafka/`, `limitRange/`, `ingress/`, `argocd/`)와 helm 릴리스는 수동 apply 대상이다.

---

## 아직 안 한 것

- **ArgoCD Application 미등록.** ArgoCD 는 떠 있지만 이 repo 를 아직 바라보지
  않는다 (`kubectl -n argocd get applications` 가 비어 있다). 지금 배포는 전부
  수동 apply 이고, GitOps 로 넘기려면 Application CR 을 `argocd/` 에 추가해야 한다.
- **앱 메트릭 미수집.** producer/consumer 용 ServiceMonitor 가 없어서 Grafana 에는
  클러스터/노드 지표만 보인다. 앱 지표를 보려면 각 Service 에 ServiceMonitor 를 붙여야 한다.
- **Secret 평문 커밋.** `secret/` 의 값이 평문이다. 학습용 클러스터라 그대로 뒀지만,
  공개 repo 에 올릴 성질의 값은 아니다.
- **Grafana / Prometheus 데이터 비영속.** `persistence.enabled: false` 라
  Pod 가 재시작되면 대시보드와 지표가 사라진다.
