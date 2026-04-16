import { S3Service } from "@aspan-corporation/ac-shared";
import { Logger } from "@aws-lambda-powertools/logger";
import { spawn } from "child_process";
import type { TagInput } from "./graphqlTypes.ts";

const FFPROBE_PATH = "/opt/bin/ffprobe";

type VideoMetaExtractorParams = {
  sourceS3Service: S3Service;
  sourceBucket: string;
  sourceKey: string;
  logger: Logger;
};

export const videoMetaExtractor = async ({
  sourceBucket,
  sourceKey,
  logger,
  sourceS3Service,
}: VideoMetaExtractorParams) => {
  logger.debug(
    `starting metadata extraction from ${sourceBucket}/${sourceKey}`,
  );

  const signedSourceUrl = await sourceS3Service.getSignedUrl({
    Bucket: sourceBucket,
    Key: sourceKey,
  });

  const ffprobe = spawn(FFPROBE_PATH, [
    "-i",
    signedSourceUrl,
    "-show_format",
    "-show_streams",
    "-v",
    "quiet",
    "-print_format",
    "json",
  ]);

  const metadataJson = await new Promise((resolve, reject) => {
    let jsonString = "";

    ffprobe.stdout.on("data", (data) => {
      jsonString += data.toString();
    });

    ffprobe.on("close", (code) => {
      if (code === 0) {
        resolve(JSON.parse(jsonString));
      } else {
        reject(new Error(`ffprobe exited with code ${code}`));
      }
    });

    ffprobe.on("error", (err) => {
      reject(err);
    });
  });

  logger.debug(
    `finished metadata extraction from ${sourceBucket}/${sourceKey}`,
  );

  const extractedMetadata = extractFromJson(metadataJson, logger);

  logger.debug("extracted metadata", { extractedMetadata });

  return extractedMetadata;
};

const extractFromJson = (parsed: any, logger: Logger): TagInput[] => {
  const raw: Array<{ key: string; value: string | undefined }> = [
    { key: "duration", value: parsed.format.duration },
    { key: "bit_rate", value: parsed.format.bit_rate },
    { key: "creation_time", value: parsed.format.tags?.creation_time },
    { key: "width", value: parsed.streams[0].width?.toString() },
    { key: "height", value: parsed.streams[0].height?.toString() },
    { key: "video_codec_name", value: parsed.streams[0].codec_name },
    { key: "video_codec_long_name", value: parsed.streams[0].codec_long_name },
    { key: "avg_frame_rate", value: parsed.streams[0].avg_frame_rate },
    { key: "audio_codec_name", value: parsed.streams[1]?.codec_name },
    { key: "audio_codec_long_name", value: parsed.streams[1]?.codec_long_name },
    { key: "audio_sample_rate", value: parsed.streams[1]?.sample_rate },
    { key: "make", value: parsed.format.tags?.["com.apple.quicktime.make"] },
    { key: "model", value: parsed.format.tags?.["com.apple.quicktime.model"] },
    {
      key: "location",
      value: parsed.format.tags?.["com.apple.quicktime.location.ISO6709"],
    },
    ...parseCoordinateString(
      parsed.format.tags?.["com.apple.quicktime.location.ISO6709"] || "",
      logger,
    ),
  ];

  return raw.filter(
    (tag): tag is TagInput => tag.value != null
  );
};

function parseCoordinateString(input: string, logger: Logger): TagInput[] {
  const regex = /([+-]\d+\.\d+)([+-]\d+\.\d+)([+-]\d+\.\d+)\//;

  const match = input.match(regex);
  if (!match) {
    return [];
  }

  const [, lat, lon, elev] = match;

  return [
    { key: "latitude", value: lat },
    { key: "longitude", value: lon },
    { key: "elevation", value: elev },
  ];
}
