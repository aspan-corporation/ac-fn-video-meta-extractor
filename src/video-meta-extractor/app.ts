import {
  AcServices,
  assertEnvVar,
  DynamoDBService,
  getIdempotencyOptions,
  getPartialResponseHandler,
  LocationService,
  makeIdempotent,
  S3Service,
  STSService,
  withMiddlewares,
} from "@aspan-corporation/ac-shared";
import type { Handler } from "aws-lambda";
import { recordHandler } from "./recordHandler.ts";

const region = process.env.AWS_REGION || "us-east-1";
const idempotentRecordHandler = makeIdempotent(
  recordHandler,
  getIdempotencyOptions(assertEnvVar("AC_IDEMPOTENCY_TABLE_NAME"), "messageId"),
);
const partialHandler = getPartialResponseHandler(idempotentRecordHandler);

export const handler: Handler = withMiddlewares(partialHandler).use({
  before: async ({ context }) => {
    const { logger } = context;
    const stsService = new STSService({ region, logger });

    const assumeRoleCommandOutput = await stsService.assumeRole({
      RoleArn: assertEnvVar("AC_TAU_MEDIA_MEDIA_BUCKET_ACCESS_ROLE_ARN"),
      RoleSessionName: "ac-fn-video-meta-extractor",
      ExternalId: "ac",
    });

    const sourceS3Service = new S3Service({
      region,
      assumeRoleCommandOutput,
      logger,
    });

    const locationService = new LocationService({
      region,
      logger,
    });
    const dynamoDBService = new DynamoDBService({
      region,
      logger,
    });

    // Same-account client (Lambda's own role) for reading in-account source
    // buckets the media read-access role can't reach — e.g. the diary bucket
    // holding diary-uploaded videos.
    const localS3Service = new S3Service({ region, logger });

    const acServices: AcServices = {
      sourceS3Service,
      locationService,
      dynamoDBService,
      localS3Service,
    };

    context.acServices = acServices;
  },
});
