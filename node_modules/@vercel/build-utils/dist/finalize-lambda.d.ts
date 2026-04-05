/// <reference types="node" />
import type { Lambda } from './lambda';
import type { NodejsLambda } from './nodejs-lambda';
import type { BytecodeCachingOptions } from './process-serverless/get-lambda-preload-scripts';
import type { SupportsStreamingResult } from './process-serverless/get-lambda-supports-streaming';
/**
 * Optional wrapper around async work, allowing callers to inject tracing
 * (e.g. dd-trace spans) without coupling the shared code to a tracer.
 */
export type TraceFn = <T>(name: string, fn: () => Promise<T>) => Promise<T>;
export interface FinalizeLambdaParams {
    lambda: Lambda | NodejsLambda;
    encryptedEnvFilename?: string;
    encryptedEnvContent?: string;
    bytecodeCachingOptions: BytecodeCachingOptions;
    forceStreamingRuntime: boolean;
    /** When true, collect the uncompressed size of lambda files before zipping. */
    enableUncompressedLambdaSizeCheck?: boolean;
    /** Optional tracing wrapper for `collectUncompressedSize` and `createZip`. */
    trace?: TraceFn;
}
export interface FinalizeLambdaResult {
    buffer: Buffer;
    digest: string;
    uncompressedBytes: number;
    /** Non-fatal streaming detection error, if any. Caller decides how to log. */
    streamingError?: SupportsStreamingResult['error'];
}
/**
 * Core Lambda finalization logic shared between BYOF and build-container.
 *
 * This function:
 * 1. Injects encrypted env file into lambda.files when provided
 * 2. Collects uncompressed size when enabled
 * 3. Creates the ZIP buffer
 * 4. Computes SHA-256 digest
 * 5. Merges environment variables (bytecode caching, helpers, etc.)
 * 6. Detects streaming support
 *
 * Note: This function mutates the `lambda` (files, environment,
 * supportsResponseStreaming).
 */
export declare function finalizeLambda(params: FinalizeLambdaParams): Promise<FinalizeLambdaResult>;
