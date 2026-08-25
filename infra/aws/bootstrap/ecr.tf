# ECR repos — standing so images persist across runs (rebuilt only when code changes).
resource "aws_ecr_repository" "repo" {
  for_each             = toset(var.ecr_repos)
  name                 = "${var.project_tag}/${each.value}"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
}

resource "aws_ecr_lifecycle_policy" "repo" {
  for_each   = aws_ecr_repository.repo
  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "expire all but the 5 most recent"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}

output "ecr_registry" {
  value = "${var.aws_account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
}
