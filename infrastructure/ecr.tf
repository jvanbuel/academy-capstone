resource "aws_ecr_repository" "foo" {
  name                 = "jan-winter-school-2024"
  image_tag_mutability = "MUTABLE"
}

