minio-run:
	minio server /tmp/minio --license ~/Documents/Database/minio/minio.license

prefect-run:
	prefect server start