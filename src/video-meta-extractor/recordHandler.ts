import {
  AcContext,
  assertEnvVar,
  hintFallbackTags,
  isAllowedVideoExtension,
  MetricUnit,
  processMeta,
} from "@aspan-corporation/ac-shared";
import type { S3ObjectCreatedNotificationEvent, SQSRecord } from "aws-lambda";
import assert from "node:assert/strict";
import { videoMetaExtractor } from "./videoMetaExtractor.ts";

const metaTableName = assertEnvVar("AC_TAU_MEDIA_META_TABLE_NAME");
const placeIndexName = assertEnvVar("AC_PLACE_INDEX_NAME");

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

  let item: S3ObjectCreatedNotificationEvent;
  try {
    item = JSON.parse(payload) as S3ObjectCreatedNotificationEvent;
  } catch (e) {
    throw new Error(
      `Failed to parse SQS record payload: ${e instanceof Error ? e.message : String(e)}`,
    );
  }

  const sourceKey = item?.detail?.object?.key;
  const size = item?.detail?.object?.size;
  const sourceBucket = item?.detail?.bucket?.name;

  assert(
    sourceKey,
    "Parsed event is missing required field: detail.object.key",
  );
  assert(
    size != null,
    "Parsed event is missing required field: detail.object.size",
  );
  assert(
    sourceBucket,
    "Parsed event is missing required field: detail.bucket.name",
  );

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

  // Attach yearImported / monthImported from the object's LastModified
  // timestamp, and fall back to device hints (x-amz-meta-hint-*, set by the
  // diary upload flow) for date/GPS ffprobe didn't find — mobile OSes strip
  // this metadata from files handed to web pages.
  const importTags: Array<{ key: string; value: string }> = [];
  const hintTags: Array<{ key: string; value: string }> = [];
  try {
    const head = await sourceS3Service.headObject({
      Bucket: sourceBucket,
      Key: sourceKey,
    });
    if (head.LastModified) {
      importTags.push(
        {
          key: "yearImported",
          value: String(head.LastModified.getUTCFullYear()),
        },
        {
          key: "monthImported",
          value: String(head.LastModified.getUTCMonth() + 1),
        },
      );
    }
    hintTags.push(...hintFallbackTags(meta, head.Metadata));
    if (hintTags.length > 0) {
      logger.debug("DeviceHintFallbackApplied", {
        sourceKey,
        keys: hintTags.map((t) => t.key),
      });
    }
  } catch (err) {
    logger.warn("HeadObjectFailed — import tags will be skipped", {
      sourceKey,
      reason: err instanceof Error ? err.message : String(err),
    });
  }

  await processMeta({
    dynamoDBService,
    locationService,
    meta: [...meta, ...hintTags, ...importTags],
    size,
    id: sourceKey,
    metaTableName,
    placeIndexName,
    logger,
  });

  logger.debug("VideoMetaExtractionsFinished", { sourceKey });
  metrics.addMetric("VideoMetaExtractionsFinished", MetricUnit.Count, 1);
};
