# test airflow behaviour

## Copy Test DAG to GCP Composer Bucket:
```
COMPOSER_BUCKET=******
gsutil -m rsync -rc ./dags gs://${COMPOSER_BUCKET}/dags
```

