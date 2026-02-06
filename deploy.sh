#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR"
echo "[dramax CD] Ejecutando desde: $(pwd)"

if [ -f deploy.config ]; then
    export $(grep -v '^#' deploy.config | xargs)
fi

REMOTE_URL=$(git config --get remote.origin.url)
AUTH_REMOTE_URL=${REMOTE_URL/https:\/\//https://$GIT_USER:$GIT_TOKEN@}
echo "📡 [dramax CD] Sincronizando con el servidor (git fetch)..."
git fetch $AUTH_REMOTE_URL

echo "🔍 [dramax CD] Buscando diferencias entre origin/$BRANCH y tu local..."
CAMBIOS=$( { git diff --name-only origin/$BRANCH; git ls-files --others --exclude-standard; } )

if [ -n "$CAMBIOS" ]; then
    echo "📢 [dramax CD] Se detectaron cambios globales en el proyecto."
    FULL_IMAGE_NAME="$REGISTRY/$PROJECT_NAME/dramax:$TAG"
    echo "📦 [dramax CD] Construyendo imagen: $FULL_IMAGE_NAME..."
    docker build -t "$FULL_IMAGE_NAME" -f "Dockerfile" .
    echo "📤 [dramax CD] Subiendo a registro..."
    docker push "$FULL_IMAGE_NAME"
else
    echo "✅ [dramax CD] No hay cambios pendientes."
fi