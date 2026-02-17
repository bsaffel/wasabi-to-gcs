#!/usr/bin/env bash
set -euo pipefail

VM_NAME="${1:?Usage: deploy.sh VM_NAME VM_ZONE VM_TYPE}"
VM_ZONE="${2:?Usage: deploy.sh VM_NAME VM_ZONE VM_TYPE}"
VM_TYPE="${3:?Usage: deploy.sh VM_NAME VM_ZONE VM_TYPE}"

# --- Validate Wasabi credentials exist in local env ---
if [[ -z "${WASABI_ACCESS_KEY:-}" ]]; then
    echo "Error: WASABI_ACCESS_KEY is not set in your environment." >&2
    echo "Export it before running deploy: export WASABI_ACCESS_KEY=\"your-key\"" >&2
    exit 1
fi
if [[ -z "${WASABI_SECRET_KEY:-}" ]]; then
    echo "Error: WASABI_SECRET_KEY is not set in your environment." >&2
    echo "Export it before running deploy: export WASABI_SECRET_KEY=\"your-key\"" >&2
    exit 1
fi

echo "==> Creating VM: ${VM_NAME} (${VM_TYPE}) in ${VM_ZONE}..."
gcloud compute instances create "$VM_NAME" \
    --zone="$VM_ZONE" \
    --machine-type="$VM_TYPE" \
    --scopes=cloud-platform \
    --image-family=debian-12 \
    --image-project=debian-cloud

# --- Wait for SSH to become available ---
echo "==> Waiting for SSH to become available..."
MAX_ATTEMPTS=30
for i in $(seq 1 "$MAX_ATTEMPTS"); do
    if gcloud compute ssh "$VM_NAME" --zone="$VM_ZONE" --command=true 2>/dev/null; then
        echo "    SSH ready."
        break
    fi
    if [[ "$i" -eq "$MAX_ATTEMPTS" ]]; then
        echo "Error: SSH not available after ${MAX_ATTEMPTS} attempts." >&2
        exit 1
    fi
    echo "    Attempt ${i}/${MAX_ATTEMPTS} — retrying in 5s..."
    sleep 5
done

# --- Write .env.tmp locally with restrictive permissions ---
echo "==> Preparing credentials file..."
(
    umask 077
    printf 'export WASABI_ACCESS_KEY=%q\nexport WASABI_SECRET_KEY=%q\n' \
        "$WASABI_ACCESS_KEY" "$WASABI_SECRET_KEY" > .env.tmp
)

# --- Copy binary, credentials, and state directory ---
echo "==> Copying binary and credentials to VM..."
gcloud compute scp wasabi-to-gcs-linux ".env.tmp" "$VM_NAME":~ --zone="$VM_ZONE"

# Remove local temp file immediately
rm -f .env.tmp

# Copy state directory if it exists
if [[ -d migration_state ]]; then
    echo "==> Copying migration_state/ to VM..."
    gcloud compute scp --recurse migration_state "$VM_NAME":~ --zone="$VM_ZONE"
fi

# --- Set permissions on the VM ---
echo "==> Setting file permissions on VM..."
gcloud compute ssh "$VM_NAME" --zone="$VM_ZONE" --command="
    mv ~/.env.tmp ~/.env && chmod 600 ~/.env && chmod +x ~/wasabi-to-gcs-linux
"

echo ""
echo "=========================================="
echo "  VM deployed successfully!"
echo "=========================================="
echo ""
echo "Connect to the VM:"
echo "  make ssh"
echo ""
echo "On the VM, start the migration:"
echo "  source ~/.env"
echo "  ./wasabi-to-gcs-linux \\"
echo "    --wasabi-endpoint https://s3.us-east-1.wasabisys.com \\"
echo "    --wasabi-region us-east-1 \\"
echo "    --wasabi-bucket YOUR_BUCKET \\"
echo "    --gcs-bucket YOUR_BUCKET"
echo ""
echo "Tip: Use tmux so you can disconnect and reconnect later:"
echo "  tmux new -s migrate"
echo "  # ... start migration ..."
echo "  # Ctrl-b d to detach, then 'make ssh' + 'tmux attach' to reconnect"
echo ""
echo "When done, tear down the VM:"
echo "  make teardown"
