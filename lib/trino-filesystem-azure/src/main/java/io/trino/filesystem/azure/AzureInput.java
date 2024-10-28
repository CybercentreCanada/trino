/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.azure;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.specialized.BlobInputStream;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoInput;

import java.io.EOFException;
import java.io.IOException;
import java.util.OptionalLong;

import static io.trino.filesystem.azure.AzureUtils.handleAzureException;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class AzureInput
        implements TrinoInput
{
    private static final Logger log = Logger.get(AzureInput.class);

    private final AzureLocation location;
    private final BlobClient blobClient;
    private OptionalLong length;
    private boolean closed;

    public AzureInput(AzureLocation location, BlobClient blobClient, OptionalLong length)
    {
        this.location = requireNonNull(location, "location is null");
        this.blobClient = requireNonNull(blobClient, "blobClient is null");
        this.length = requireNonNull(length, "length is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        if (bufferLength == 0) {
            return;
        }

        BlobInputStreamOptions options = new BlobInputStreamOptions()
                .setRange(new BlobRange(position, (long) bufferLength))
                .setBlockSize(bufferLength);

        final int maxRetries = 3;
        int attempts = 0;

        while (attempts < maxRetries) {
            attempts++;
            try (BlobInputStream blobInputStream = blobClient.openInputStream(options)) {
                long fileSize = blobInputStream.getProperties().getBlobSize();
                if (position >= fileSize) {
                    throw new IOException("Cannot read at %s. File size is %s: %s".formatted(position, fileSize, location));
                }

                int readSize = blobInputStream.readNBytes(buffer, bufferOffset, bufferLength);
                if (readSize != bufferLength) {
                    throw new EOFException("End of file reached before reading fully: " + location);
                }

                // Successfully read, so break out of the retry loop
                break;
            }
            catch (IllegalStateException e) {
                if (attempts >= maxRetries) {
                    throw new IOException("Failed to read file due to an unbalanced enter/exit error after " + attempts + " attempts", e);
                }
                log.warn("Attempt %s failed due to IllegalStateException, retrying... Location: %s", attempts, location, e);
                // Short pause to avoid hammering the server on retries
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during retry sleep", ie);
                }
            }
            catch (RuntimeException e) {
                throw handleAzureException(e, "reading file", location);
            }
        }
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);

        try {
            if (length.isEmpty()) {
                length = OptionalLong.of(blobClient.getProperties().getBlobSize());
            }
            BlobInputStreamOptions options = new BlobInputStreamOptions()
                    .setRange(new BlobRange(length.orElseThrow() - bufferLength))
                    .setBlockSize(bufferLength);
            try (BlobInputStream blobInputStream = blobClient.openInputStream(options)) {
                return blobInputStream.readNBytes(buffer, bufferOffset, bufferLength);
            }
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "reading file", location);
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
