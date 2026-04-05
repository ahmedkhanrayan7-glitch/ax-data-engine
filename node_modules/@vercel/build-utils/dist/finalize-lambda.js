"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);
var finalize_lambda_exports = {};
__export(finalize_lambda_exports, {
  finalizeLambda: () => finalizeLambda
});
module.exports = __toCommonJS(finalize_lambda_exports);
var import_get_encrypted_env_file = require("./process-serverless/get-encrypted-env-file");
var import_get_lambda_environment = require("./process-serverless/get-lambda-environment");
var import_get_lambda_supports_streaming = require("./process-serverless/get-lambda-supports-streaming");
var import_stream_to_digest_async = require("./fs/stream-to-digest-async");
var import_collect_uncompressed_size = require("./collect-uncompressed-size");
const defaultTrace = (_name, fn) => fn();
async function finalizeLambda(params) {
  const {
    lambda,
    encryptedEnvFilename,
    encryptedEnvContent,
    bytecodeCachingOptions,
    forceStreamingRuntime,
    enableUncompressedLambdaSizeCheck,
    trace = defaultTrace
  } = params;
  const encryptedEnv = (0, import_get_encrypted_env_file.getEncryptedEnv)(
    encryptedEnvFilename,
    encryptedEnvContent
  );
  if (encryptedEnv) {
    const [envFilename, envFile] = encryptedEnv;
    lambda.zipBuffer = void 0;
    lambda.files = {
      ...lambda.files,
      [envFilename]: envFile
    };
  }
  let uncompressedBytes = 0;
  if (enableUncompressedLambdaSizeCheck) {
    if (lambda.files) {
      uncompressedBytes = await trace(
        "collectUncompressedSize",
        () => (0, import_collect_uncompressed_size.collectUncompressedSize)(lambda.files ?? {})
      );
    }
  }
  const buffer = lambda.zipBuffer || await trace("createZip", () => lambda.createZip());
  const digest = (0, import_stream_to_digest_async.sha256)(buffer);
  lambda.environment = {
    ...lambda.environment,
    ...(0, import_get_lambda_environment.getLambdaEnvironment)(lambda, buffer, bytecodeCachingOptions)
  };
  const streamingResult = await (0, import_get_lambda_supports_streaming.getLambdaSupportsStreaming)(
    lambda,
    forceStreamingRuntime
  );
  lambda.supportsResponseStreaming = streamingResult.supportsStreaming;
  return {
    buffer,
    digest,
    uncompressedBytes,
    streamingError: streamingResult.error
  };
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  finalizeLambda
});
