import { QueueLambdaConstruct } from "@aspan-corporation/ac-shared-cdk";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as logs from "aws-cdk-lib/aws-logs";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";
import { fileURLToPath } from "node:url";
import * as path from "path";

const currentFilePath = fileURLToPath(import.meta.url);
const currentDirPath = path.dirname(currentFilePath);

export class AcFnVideoMetaExtractorStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Get FFprobe layer ARN from SSM
    const ffprobeLayerArn = ssm.StringParameter.valueForStringParameter(
      this,
      "/ac/layers/ffprobe/arn",
    );

    // Get centralized log group from monitoring stack
    const centralLogGroupArn = ssm.StringParameter.valueForStringParameter(
      this,
      "/ac/monitoring/central-log-group-arn",
    );
    const centralLogGroup = logs.LogGroup.fromLogGroupArn(
      this,
      "CentralLogGroup",
      centralLogGroupArn,
    );

    // Create the Queue + Lambda construct for video metadata extraction processing
    const videoMetaExtractorProcessor = new QueueLambdaConstruct(
      this,
      "VideoMetaExtractorProcessor",
      {
        entry: path.join(currentDirPath, "../src/video-meta-extractor/app.ts"),
        handler: "handler",
        logGroup: centralLogGroup,
        memorySize: 3008,
        timeout: cdk.Duration.seconds(400),
        batchSize: 1,
        maxReceiveCount: 10,
        layers: [
          lambda.LayerVersion.fromLayerVersionArn(
            this,
            "FFprobeLayer",
            ffprobeLayerArn,
          ),
        ],
        environment: {
          LOG_LEVEL: "INFO",
          POWERTOOLS_SERVICE_NAME: "ac-fn-video-meta-extractor",
          AC_IDEMPOTENCY_TABLE_NAME:
            ssm.StringParameter.valueForStringParameter(
              this,
              "/ac/data/idempotency-table-name",
            ),
          AC_TAU_MEDIA_META_TABLE_NAME:
            ssm.StringParameter.valueForStringParameter(
              this,
              "/ac/data/meta-table-name",
            ),
          AC_TAU_MEDIA_MEDIA_BUCKET_ACCESS_ROLE_ARN:
            ssm.StringParameter.valueForStringParameter(
              this,
              "/ac/iam/media-bucket-access-role-arn",
            ),
        },
      },
    );

    const idempotencyTableName = ssm.StringParameter.valueForStringParameter(
      this,
      "/ac/data/idempotency-table-name",
    );

    const idempotencyTableArn = cdk.Arn.format(
      {
        partition: "aws",
        service: "dynamodb",
        region: this.region,
        account: this.account,
        resource: `table/${idempotencyTableName}`,
      },
      this,
    );

    videoMetaExtractorProcessor.processor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:GetItem",
          "dynamodb:DeleteItem",
          "dynamodb:DescribeTable",
          "dynamodb:ConditionCheckItem",
        ],
        resources: [idempotencyTableArn],
      }),
    );

    const metaTableName = ssm.StringParameter.valueForStringParameter(
      this,
      "/ac/data/meta-table-name",
    );

    const metaTableArn = cdk.Arn.format(
      {
        partition: "aws",
        service: "dynamodb",
        region: this.region,
        account: this.account,
        resource: `table/${metaTableName}`,
      },
      this,
    );

    videoMetaExtractorProcessor.processor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:GetItem",
          "dynamodb:DeleteItem",
          "dynamodb:DescribeTable",
          "dynamodb:ConditionCheckItem",
        ],
        resources: [metaTableArn],
      }),
    );

    const placeIndexArn = cdk.Arn.format(
      {
        partition: "aws",
        service: "geo",
        region: this.region,
        account: this.account,
        resource: "place-index/TauPlaceIndex",
      },
      this,
    );

    videoMetaExtractorProcessor.processor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["geo:SearchPlaceIndexForPosition"],
        resources: [placeIndexArn],
      }),
    );

    // Allow Lambda to assume the S3 media read access role
    videoMetaExtractorProcessor.processor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sts:AssumeRole"],
        resources: [
          "arn:aws:iam::433003433222:role/aspan-corporation/ac-s3-media-read-access",
        ],
      }),
    );

    // Store the queue URL in SSM Parameter Store for external access
    new ssm.StringParameter(
      this,
      "VideoMetaExtractorProcessorQueueUrlParameter",
      {
        parameterName: "/ac/video-meta-extractor/queue-url",
        stringValue: videoMetaExtractorProcessor.queue.queueUrl,
      },
    );
  }
}
