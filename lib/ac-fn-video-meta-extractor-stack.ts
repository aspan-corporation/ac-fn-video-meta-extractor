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

    // Get FFmpeg layer ARN from SSM (includes both ffmpeg and ffprobe)
    const ffmpegLayerArn = ssm.StringParameter.valueForStringParameter(
      this,
      "/ac/layers/ffmpeg/arn",
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
        maxReceiveCount: 3,
        reservedConcurrentExecutions: 5,

        layers: [
          lambda.LayerVersion.fromLayerVersionArn(
            this,
            "FFmpegLayer",
            ffmpegLayerArn,
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
          AC_PLACE_INDEX_NAME: "MyPlaceIndex",
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
        resource: "place-index/MyPlaceIndex",
      },
      this,
    );

    videoMetaExtractorProcessor.processor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["geo:SearchPlaceIndexForPosition"],
        resources: [placeIndexArn],
      }),
    );

    // Read grant for the consolidated media bucket (media/ and diary/ alike —
    // /ac/storage/diary-bucket-arn resolves to the same bucket as
    // /ac/storage/media-bucket-name since the cutover to MediaBucket). No
    // cross-account assume-role needed any more; the Lambda's own execution
    // role reads it directly.
    const mediaBucketArn = ssm.StringParameter.valueForStringParameter(
      this,
      "/ac/storage/diary-bucket-arn",
    );

    videoMetaExtractorProcessor.processor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetObject"],
        resources: [`${mediaBucketArn}/*`],
      }),
    );

    // Store the queue URL and ARN in SSM Parameter Store for external access
    new ssm.StringParameter(
      this,
      "VideoMetaExtractorProcessorQueueUrlParameter",
      {
        parameterName: "/ac/video-meta-extractor/queue-url",
        stringValue: videoMetaExtractorProcessor.queue.queueUrl,
      },
    );

    new ssm.StringParameter(
      this,
      "VideoMetaExtractorProcessorQueueArnParameter",
      {
        parameterName: "/ac/video-meta-extractor/queue-arn",
        stringValue: videoMetaExtractorProcessor.queue.queueArn,
      },
    );
  }
}
