/*!
 * Copyright 2026 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {ServerDuplexStream, status, ServiceError} from '@grpc/grpc-js';
import {trace, context, Tracer} from '@opentelemetry/api';
import {CloudClientExecutor} from './cloud-client-executor';
import * as protos from '../../protos/protos';
import spanner = protos.google.spanner;
import SpannerAsyncActionRequest = spanner.executor.v1.SpannerAsyncActionRequest;
import SpannerAsyncActionResponse = spanner.executor.v1.SpannerAsyncActionResponse;

/**
 * Implements the SpannerExecutorProxy service, which handles asynchronous
 * Spanner actions via a bidirectional gRPC stream.
 */
export class CloudExecutorImpl {
  private clientExecutor: CloudClientExecutor;
  private tracer: Tracer;

  constructor() {
    this.clientExecutor = new CloudClientExecutor();

    this.tracer = trace.getTracer(CloudClientExecutor.name);
  }

  /**
   * Handles incoming SpannerAsyncActionRequest messages from the client.
   */
  public executeActionAsync(
    call: ServerDuplexStream<
      SpannerAsyncActionRequest,
      SpannerAsyncActionResponse
    >,
  ): void {
    // Create a top-level OpenTelemetry span for streaming request.
    const span = this.tracer.startSpan(
      'nodejs_systest_execute_actions_stream',
      {
        root: true,
      },
    );

    const streamContext = trace.setSpan(context.active(), span);

    // The executionContext manages the lifecycle and flow state for this specific gRPC stream context.
    const executionContext =
      this.clientExecutor.createExecutionFlowContext(call);

    // Handle receiving requests on duplex stream
    // Handle incoming requests sequentially on the duplex stream.
    call.on('data', (request: SpannerAsyncActionRequest) => {
      context.with(streamContext, () => {
        console.log(`Receiving request: \n${JSON.stringify(request, null, 2)}`);

        // Ensure nested properties exist before attempting to inject configuration overrides.
        if (!request.action) request.action = {};
        if (!request.action.spannerOptions) request.action.spannerOptions = {};
        if (!request.action.spannerOptions.sessionPoolOptions)
          request.action.spannerOptions.sessionPoolOptions = {};

        // as the multiplexed session is defualt enabled, mutate the incoming request to forcefully enforce multiplexed sessions for this proxy execution.
        request.action.spannerOptions.sessionPoolOptions.useMultiplexed = true;

        console.log(
          `Updated request to set multiplexed session flag: \n${JSON.stringify(request, null, 2)}`,
        );

        // TODO: Set requestHasReadOrQueryAction flag here when Read/Query are implemented.

        try {
          const reqStatus = this.clientExecutor.startHandlingRequest(
            request,
            executionContext,
          );
          if (reqStatus.code !== status.OK) {
            console.error(
              `Failed to handle request, half closed: ${reqStatus.details}`,
            );
          }
        } catch (err) {
          console.error('Exception when handling request', err);
        }
      });
    });

    // Handle stream errors
    call.on('error', (err: Error) => {
      context.with(streamContext, () => {
        console.error('Client ends the stream with error.', err);
        span.recordException(err);
        span.end();
        executionContext.cleanup();
      });
    });

    // Handle the completion of the client stream
    call.on('end', async () => {
      await context.with(streamContext, async () => {
        span.end();
        // TODO: Add End-to-End trace verification here once Read/Query actions are implemented.
        console.log('Client called Done, half closed');
        executionContext.cleanup();

        call.end();
      });
    });
  }
}
