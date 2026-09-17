#!/usr/bin/env bash
# 설정(Secret) 변경을 감지해서 무중단 롤링 업데이트를 트리거한다.
#
# 왜 필요한가:
#   Deployment 는 Secret 의 "이름"만 참조하므로, Secret 내용이 바뀌어도
#   pod template 이 그대로다 -> 새 ReplicaSet 이 안 생김 -> 롤아웃 없음.
#   그래서 내용의 해시를 template annotation 에 넣어 template 자체를 바꿔준다.
#
# 네임스페이스:
#   semi-app = 앱(producer/consumer) + kafka Service + 앱용 Secret
#   semi-db  = mysql/mongodb + DB용 Secret + PVC
#   각 매니페스트에 metadata.namespace 가 박혀 있지만, -n 을 같이 넘겨서
#   누가 그 필드를 지웠을 때 조용히 default 로 가지 않고 에러가 나게 한다.
#
# 이 스크립트가 다루지 않는 것 (최초 1회 수동 적용):
#   kubectl apply -f pv/     # PV 는 클러스터 스코프, PVC 는 semi-db
#   kubectl apply -f db/     # StatefulSet 은 롤링 업데이트 대상이 아님
#   kubectl apply -f kafka/
#   kubectl apply -f limitRange/
#   kubectl apply -f ingress/
#
# 사용법:
#   ./rollout.sh            # 변경분만 롤아웃
#   ./rollout.sh --force    # 변경이 없어도 무조건 재시작
set -euo pipefail

cd "$(dirname "$0")"

APP_NS="semi-app"
DB_NS="semi-db"

# Secret 파일이 네임스페이스별로 갈렸다
APP_SECRET_FILES=(secret/application-secrets.yaml)
DB_SECRET_FILES=(secret/db-secrets.yaml)

DEPLOYMENTS=(consumer-deployment producer-deployment)

FORCE=0
[[ "${1:-}" == "--force" ]] && FORCE=1

# 0) 네임스페이스가 없으면 Secret 적용부터 실패하므로 먼저 보장한다 (멱등)
kubectl apply -f ns/

# 1) Secret 을 먼저 반영한다
for f in "${APP_SECRET_FILES[@]}"; do
  kubectl apply -n "$APP_NS" -f "$f"
done
for f in "${DB_SECRET_FILES[@]}"; do
  kubectl apply -n "$DB_NS" -f "$f"
done

# 2) 설정 내용의 해시를 계산한다.
#    앱 Deployment 가 envFrom 으로 참조하는 것은 application-secrets.yaml 뿐이므로
#    이것만 해시한다. db-secrets.yaml 은 StatefulSet 쪽이라 여기서 롤아웃 대상이
#    아니고, 포함시키면 DB 비번만 바꿔도 앱이 불필요하게 재시작된다.
HASH=$(cat "${APP_SECRET_FILES[@]}" | sha256sum | cut -c1-16)
echo "config checksum: $HASH"

# 3) Deployment 를 반영하고, annotation 을 해시로 갱신한다
kubectl apply -n "$APP_NS" -f app/

for d in "${DEPLOYMENTS[@]}"; do
  CURRENT=$(kubectl get deploy "$d" -n "$APP_NS" \
    -o jsonpath='{.spec.template.metadata.annotations.checksum/config}' 2>/dev/null || echo "")

  if [[ "$CURRENT" == "$HASH" && "$FORCE" -eq 0 ]]; then
    echo "  $d: 변경 없음 (skip)"
    continue
  fi

  echo "  $d: $CURRENT -> $HASH"
  kubectl patch deploy "$d" -n "$APP_NS" --type=merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"checksum/config\":\"$HASH\"}}}}}"
done

# 4) 롤아웃이 끝날 때까지 기다린다 (실패하면 0 이 아닌 코드로 종료)
for d in "${DEPLOYMENTS[@]}"; do
  kubectl rollout status "deploy/$d" -n "$APP_NS" --timeout=5m
done

echo "완료."
