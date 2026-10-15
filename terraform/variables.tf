variable "aws_region" {
  description = "Região da AWS onde os recursos serão criados"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Nome do projeto"
  type        = string
  default     = "befly-medallion"
}

variable "environment" {
  description = "Ambiente de implantação"
  type        = string
  default     = "dev"
}