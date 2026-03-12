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

import * as fs from 'fs';
import * as grpc from '@grpc/grpc-js';
import {WorkerProxy} from './worker-proxy';
import { SpannerOptions } from '../../src';

/**
 * Provides utility methods for configuring the Cloud Spanner client for tests.
 */
export class CloudUtil {
  // If this is set too low, the peer server may return RESOURCE_EXHAUSTED errors if the response
  // error message causes the trailing headers to exceed this limit.
  private static readonly GRPC_MAX_HEADER_LIST_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB

  private static readonly TEST_HOST_IN_CERT = 'test-cert-2';

  /**
   * Creates the configuration object for the Spanner client for connecting to a
   * test GFE, including gRPC channel setup.
   */
  public static getSpannerOptions(): any {
    const options: SpannerOptions = {
      projectId: WorkerProxy.PROJECT_ID,
      servicePath: 'localhost',
      port: WorkerProxy.spannerPort,
    };

    const maxMessageSize = 100 * 1024 * 1024;

    const grpcOptions: grpc.ClientOptions = {
      'grpc.max_receive_message_length': maxMessageSize,
      'grpc.max_metadata_size': this.GRPC_MAX_HEADER_LIST_SIZE_BYTES,
    };

    if (WorkerProxy.usePlainTextChannel) {
      options.sslCreds = grpc.credentials.createInsecure();
    } else {
      const rootCerts = CertUtil.copyCert(WorkerProxy.cert);
      options.sslCreds = grpc.credentials.createSsl(rootCerts);

      // Override authority to match the test certificate.
      // In Node.js gRPC:
      // - ssl_target_name_override is used for the SSL handshake check (CN/SAN matching).
      // - default_authority is used for the HTTP/2 :authority header.
      (grpcOptions as grpc.ChannelOptions)['grpc.ssl_target_name_override'] =
        this.TEST_HOST_IN_CERT;
      (grpcOptions as grpc.ChannelOptions)['grpc.default_authority'] = this.TEST_HOST_IN_CERT;
    }

    (options as any).grpcOptions = grpcOptions;

    return options;
  }
}

/**
 * A utility class for handling certificates.
 */
export class CertUtil {
  /**
   * Reads a certificate file from the provided path and copies its contents into a buffer,
   * deliberately stripping out any arbitrary content outside the BEGIN/END blocks.
   */
  public static copyCert(certPath: string): Buffer {
    try {
      const certContent = fs.readFileSync(certPath, 'utf8');
      const lines = certContent.split(/\r?\n/);
      let cleanCert = '';
      let inCert = false;

      for (const line of lines) {
        const trimmedLine = line.trim();

        if (trimmedLine === '-----BEGIN CERTIFICATE-----') {
          inCert = true;
        }

        if (inCert) {
          cleanCert += line + '\n';
        }

        if (trimmedLine === '-----END CERTIFICATE-----') {
          inCert = false;
        }
      }
      return Buffer.from(cleanCert);
    } catch (e) {
      throw new Error(`Failed to read certificate from ${certPath}: ${e}`);
    }
  }
}
