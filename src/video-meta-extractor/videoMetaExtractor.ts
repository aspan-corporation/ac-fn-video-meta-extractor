import { S3Service } from "@aspan-corporation/ac-shared";
import { Logger } from "@aws-lambda-powertools/logger";
import { spawn } from "child_process";
import type { TagInput } from "./graphqlTypes.ts";

const FFPROBE_PATH = "/opt/bin/ffprobe";

interface FfprobeOutput {
  format: {
    duration?: string;
    bit_rate?: string;
    tags?: {
      creation_time?: string;
      location?: string;
      "com.apple.quicktime.location.ISO6709"?: string;
    };
  };
  streams: Array<{
    codec_type?: string;
    codec_name?: string;
    codec_long_name?: string;
    width?: number;
    height?: number;
    avg_frame_rate?: string;
    sample_rate?: string;
    channels?: number;
  }>;
}

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

  const ffprobe = spawn(
    FFPROBE_PATH,
    [
      "-i",
      signedSourceUrl,
      "-show_format",
      "-show_streams",
      "-v",
      "quiet",
      "-print_format",
      "json",
    ],
    { timeout: 30000 },
  );

  const metadataJson = await new Promise<FfprobeOutput>((resolve, reject) => {
    const MAX_BUFFER = 10 * 1024 * 1024; // 10MB
    let jsonString = "";

    ffprobe.stdout.on("data", (data) => {
      if (jsonString.length < MAX_BUFFER) {
        jsonString += data.toString();
      }
    });

    ffprobe.on("close", (code) => {
      if (code === 0) {
        try {
          resolve(JSON.parse(jsonString) as FfprobeOutput);
        } catch (e) {
          reject(
            new Error(
              `Failed to parse ffprobe output: ${e instanceof Error ? e.message : String(e)}`,
            ),
          );
        }
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

const extractFromJson = (parsed: FfprobeOutput, logger: Logger): TagInput[] => {
  if (!parsed.streams || parsed.streams.length === 0) return [];

  const stream0 = parsed.streams[0];
  const stream1 = parsed.streams.length > 1 ? parsed.streams[1] : undefined;

  const raw: Array<{ key: string; value: string | undefined }> = [
    { key: "duration", value: parsed.format.duration },
    { key: "bit_rate", value: parsed.format.bit_rate },
    { key: "creation_time", value: parsed.format.tags?.creation_time },
    { key: "width", value: stream0.width?.toString() },
    { key: "height", value: stream0.height?.toString() },
    { key: "video_codec_name", value: stream0.codec_name },
    { key: "video_codec_long_name", value: stream0.codec_long_name },
    { key: "avg_frame_rate", value: stream0.avg_frame_rate },
    { key: "audio_codec_name", value: stream1?.codec_name },
    { key: "audio_codec_long_name", value: stream1?.codec_long_name },
    { key: "audio_sample_rate", value: stream1?.sample_rate },
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
    logger.debug("Failed to parse coordinate", { input });
    return [];
  }

  const [, lat, lon, elev] = match;

  return [
    { key: "latitude", value: lat },
    { key: "longitude", value: lon },
    { key: "elevation", value: elev },
  ];
}
