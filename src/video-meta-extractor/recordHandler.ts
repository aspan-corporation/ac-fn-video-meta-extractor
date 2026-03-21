import {
  AcContext,
  assertEnvVar,
  isAllowedVideoExtension,
  MetricUnit,
  processMeta,
} from "@aspan-corporation/ac-shared";
import type { S3ObjectCreatedNotificationEvent, SQSRecord } from "aws-lambda";
import assert from "node:assert/strict";
import { videoMetaExtractor } from "./videoMetaExtractor.ts";

const metaTableName = assertEnvVar("AC_TAU_MEDIA_META_TABLE_NAME");

export const recordHandler = async (
  record: SQSRecord,
  context: AcContext,
): Promise<void> => {
  const { logger, metrics, acServices = {} } = context;
  const { sourceS3Service, locationService, dynamoDBService } = acServices;
  assert(sourceS3Service, "sourceS3Service is required in context.acServices");
  assert(locationService, "locationService is required in context.acServices");
  assert(dynamoDBService, "dynamoDBService is required in context.acServices");

  const payload = record.body;
  assert(payload, "SQS record has no body");

  const item = JSON.parse(payload);
  const {
    detail: {
      object: { key: sourceKey, size },
      bucket: { name: sourceBucket },
    },
  } = item as S3ObjectCreatedNotificationEvent;

  logger.debug("VideoMetaExtractionsStarted", { sourceKey });
  metrics.addMetric("VideoMetaExtractionsStarted", MetricUnit.Count, 1);

  assert(
    isAllowedVideoExtension(sourceKey),
    `extension for ${sourceKey} is not supported`,
  );

  const meta = await videoMetaExtractor({
    sourceBucket,
    sourceKey,
    sourceS3Service,
    logger,
  });

  await processMeta({
    dynamoDBService,
    locationService,
    meta,
    size,
    id: sourceKey,
    metaTableName,
    logger,
  });

  logger.debug("VideoMetaExtractionsFinished", { sourceKey });
  metrics.addMetric("VideoMetaExtractionsFinished", MetricUnit.Count, 1);
};
