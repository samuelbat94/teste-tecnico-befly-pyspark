# Gerador de sufixo aleatório para garantir nome único no S3
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# Bucket S3 do Data Lake (Armazena Raw, Bronze, Silver, Gold e Scripts)
resource "aws_s3_bucket" "datalake" {
  bucket        = "${var.project_name}-datalake-${var.environment}-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

# Bloqueio de acesso público no S3 (Boas práticas de segurança)
resource "aws_s3_bucket_public_access_block" "datalake_privacy" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Pastas Lógicas (Prefixos) dentro do Bucket
resource "aws_s3_object" "folders" {
  for_each = toset(["raw/", "bronze/", "silver/", "gold/", "scripts/", "scripts/modules/"])
  bucket   = aws_s3_bucket.datalake.id
  key      = each.value
}

# IAM Role para o AWS Glue
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

# Anexar política padrão do AWS Glue
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Política do IAM para leitura/escrita no Bucket do Data Lake
resource "aws_iam_policy" "s3_datalake_policy" {
  name        = "${var.project_name}-s3-policy"
  description = "Acesso de leitura e escrita no bucket do Data Lake"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.datalake.arn,
          "${aws_s3_bucket.datalake.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.s3_datalake_policy.arn
}

# AWS Glue Catalog Database (Catálogo de Metadados)
resource "aws_glue_catalog_database" "befly_db" {
  name        = "befly_medallion_db_${var.environment}"
  description = "Catálogo de dados para a arquitetura Medalhão da BeFly"
}

# AWS Glue Job (Job PySpark Serverless)
resource "aws_glue_job" "pyspark_pipeline" {
  name              = "${var.project_name}-pipeline-job"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0" # PySpark 3.3.0 + Python 3.10
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.datalake.id}/scripts/main.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.datalake.id}/scripts/modules.zip"
    "--BASE_PATH"                        = "s3://${aws_s3_bucket.datalake.id}"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}