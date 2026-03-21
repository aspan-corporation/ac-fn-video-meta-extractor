#!/usr/bin/env node
import * as cdk from "aws-cdk-lib/core";
import { AcFnVideoMetaExtractorStack } from "../lib/ac-fn-video-meta-extractor-stack.ts";

const app = new cdk.App();
new AcFnVideoMetaExtractorStack(app, "AcFnVideoMetaExtractorStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
