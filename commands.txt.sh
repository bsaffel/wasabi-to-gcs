go run . --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket my-source \
          --gcs-bucket my-destination \
          --prefix "images/" \

go run . --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket partb \
          --gcs-bucket partb \
          --prefix "bnb/incoming/2021-10" \
          --workers 8 \
          --speedtest

go run . --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket partb \
          --gcs-bucket partb \
          --prefix "bnb/incoming/2021-10" \
          --workers 8

go run . --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket partb \
          --gcs-bucket partb \
          --prefix "bnb/incoming/2021-11" \
          --workers 16

# ========================= Local Commands =========================

# Local Binary - Migrate w/ GCS State
./wasabi-to-gcs --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket partb \
          --gcs-bucket partb \
          --state-gcs gs://partb/wasabi_migration/ \
          --workers 16 \
          --prefix "bnb/incoming/2023-02"

# Local Binary - Compare bucket
./wasabi-to-gcs --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket partb \
          --gcs-bucket partb \
          --compare

# ========================= GCE Commands =========================

./wasabi-to-gcs-linux --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket partb \
          --gcs-bucket partb \
          --prefix "bnb/incoming/2022-10" \
          --speedtest

source .env && \
./wasabi-to-gcs-linux --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket partb \
          --gcs-bucket partb \
          --prefix "bnb/incoming/" \
          --compare


source .env && \
./wasabi-to-gcs-linux --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket partb \
          --gcs-bucket partb \
          --state-gcs gs://partb/wasabi_migration/ \
          --workers 8 \
          --prefix "bnb/incoming/2021-03" 

# GCE - Migrate w/ GCS State + Compare
source .env && \
./wasabi-to-gcs-linux --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket partb \
          --gcs-bucket partb \
          --state-gcs gs://partb/wasabi_migration/ \
          --workers 8 \
          --prefix "bnb/finished/2024" \
          --compare 

# GCE BNB - Migrate w/ GCS State + Check Destination
source .env && \
./wasabi-to-gcs-linux --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket partb \
          --gcs-bucket partb \
          --state-gcs gs://partb/wasabi_migration/ \
          --workers 8 \
          --prefix "bnb/finished/2024" \
          --check-destination

# GCE PDAC - Migrate w/ GCS State + Compare
source .env && \
./wasabi-to-gcs-linux --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket partb \
          --gcs-bucket pdac-reports \
          --state-gcs gs://pdac-reports/wasabi_migration/ \
          --workers 8 \
          --prefix "pdac/incoming" \
          --compare


