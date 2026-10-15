output "s3_bucket_name" {
  description = "Nome do Bucket gerado no S3"
  value       = aws_s3_bucket.datalake.id
}

output "glue_job_name" {
  description = "Nome do Glue Job gerado"
  value       = aws_glue_job.pyspark_pipeline.name
}

output "glue_database_name" {
  description = "Nome do Glue Catalog Database"
  value       = aws_glue_catalog_database.befly_db.name
}