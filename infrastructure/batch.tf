resource "aws_batch_job_definition" "job" {
  name                  = "Jan-winter-school-capstone"
  type                  = "container"
  platform_capabilities = ["EC2"]
  container_properties = jsonencode({
    command            = ["python3", "src/capstone/ingest.py"],
    image              = "338791806049.dkr.ecr.eu-west-1.amazonaws.com/jan-winter-school-2024:latest"
    jobRoleArn         = data.aws_iam_role.ecs_task_execution_role.arn
    execution_role_arn = data.aws_iam_role.ecs_task_execution_role.arn


    resourceRequirements = [
      {
        type  = "VCPU"
        value = "1"
      },
      {
        type  = "MEMORY"
        value = "2048"
      }
    ]
  })

}


data "aws_iam_role" "ecs_task_execution_role" {
  name = "academy-capstone-winter-2024-batch-job-role"
}


