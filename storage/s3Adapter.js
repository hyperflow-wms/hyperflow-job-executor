'use strict';

const { S3Client, ListObjectsV2Command, GetObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const fs = require('node:fs');
const fsp = fs.promises;
const path = require('node:path');
const { pipeline } = require('node:stream/promises');
const minimatch = require('minimatch');

function bool(v, def = false) {
    if (v === undefined || v === null) return def;
    if (typeof v === 'boolean') return v;
    const s = String(v).toLowerCase();
    return s === '1' || s === 'true' || s === 'yes';
}

function int(v, def) {
    const n = Number.parseInt(v, 10);
    return Number.isFinite(n) ? n : def;
}

function makeS3ClientFromEnv() {
    const endpoint = process.env.HF_S3_ENDPOINT || undefined;
    const forcePathStyle = bool(process.env.HF_S3_FORCE_PATH_STYLE, false);
    const region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1';
    // Credentials: SDK v3 reads AWS_* envs automatically.
    return new S3Client({ region, endpoint, forcePathStyle });
}

function ensureDirSync(dir) {
    fs.mkdirSync(dir, { recursive: true });
}

function relPathForKey(prefix, key) {
    if (!prefix) return path.basename(key);
    const cleanPrefix = prefix.endsWith('/') ? prefix : `${prefix}/`;
    if (key.startsWith(cleanPrefix)) {
        const rel = key.slice(cleanPrefix.length);
        return rel.length ? rel : path.basename(key);
    }
    return path.basename(key);
}

class S3Adapter {
    constructor(opts = {}) {
        this.s3 = opts.s3 || makeS3ClientFromEnv();
        this.concurrency = int(process.env.HF_S3_CONCURRENCY, 6);
        this.retries = int(process.env.HF_S3_RETRIES, 3);
        this.logger = opts.logger || console;
    }

    async list({ bucket, prefix = '', recursive = true, include = [], exclude = [], maxFiles }) {
        const out = [];
        let ContinuationToken = undefined;

        do {
            const cmd = new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: prefix,
                ContinuationToken,
                Delimiter: recursive ? undefined : '/'
            });

            const resp = await this.s3.send(cmd);

            for (const obj of resp.Contents || []) {
                const key = obj.Key;
                if (!key) continue;

                if (include.length > 0) {
                    const matchesInclude = include.some((gl) => minimatch(key, gl));
                    if (!matchesInclude) continue;
                }
                if (exclude.length > 0) {
                    const matchesExclude = exclude.some((gl) => minimatch(key, gl));
                    if (matchesExclude) continue;
                }

                out.push({
                    key,
                    size: obj.Size ?? null,
                    etag: obj.ETag ?? null
                });

                if (maxFiles && out.length >= maxFiles) {
                    if (out.length === 0) {
                        this.logger.warn('S3 list returned 0 objects', JSON.stringify({ bucket, prefix }));
                    }
                    return out;
                }
            }

            // If not recursive and we have CommonPrefixes, the caller can use them for "grouping: prefix".
            if (!out.commonPrefixes) out.commonPrefixes = [];
            if (!recursive) {
                for (const p of resp.CommonPrefixes || []) {
                    if (p.Prefix) out.commonPrefixes.push(p.Prefix);
                }
            }

            ContinuationToken = resp.IsTruncated ? resp.NextContinuationToken : undefined;
        } while (ContinuationToken);

        if (out.length === 0) {
            this.logger.warn('S3 list returned 0 objects', JSON.stringify({ bucket, prefix }));
        }

        return out;
    }

    async exists({ bucket, key }) {
        try {
            await this.s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
            return true;
        } catch (e) {
            // 404/NotFound → false; rethrow all other errors.
            const is404 = e && (e.$metadata?.httpStatusCode === 404 || e.name === 'NotFound');
            if (is404) return false;

            this.logger.error('S3 exists() unexpected error', JSON.stringify({ bucket, key, err: String(e) }));
            throw e;
        }
    }

    async downloadToPath({ bucket, key, destPath }) {
        ensureDirSync(path.dirname(destPath));

        let lastErr;
        for (let i = 0; i <= this.retries; i++) {
            try {
                const res = await this.s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
                await pipeline(res.Body, fs.createWriteStream(destPath));
                const st = await fsp.stat(destPath).catch(() => null);
                return st?.size ?? null;
            } catch (e) {
                lastErr = e;
                if (i === this.retries) {
                    this.logger.error('S3 download failed after retries', JSON.stringify({ bucket, key, destPath, err: String(e) }));
                    throw e;
                }
                const backoffMs = (i + 1) * 400;
                this.logger.warn('S3 download retry scheduled', JSON.stringify({ bucket, key, attempt: i + 1, retries: this.retries, backoffMs }));
                await new Promise((r) => setTimeout(r, backoffMs));
            }
        }
        throw lastErr;
    }

    async uploadFromPath({ bucket, key, srcPath, overwrite = false, contentType }) {
        if (!overwrite) {
            const exists = await this.exists({ bucket, key });
            if (exists) {
                this.logger.warn('S3 upload skipped: key exists and overwrite=false', JSON.stringify({ bucket, key }));
                throw new Error(`S3 key exists and overwrite=false: s3://${bucket}/${key}`);
            }
        }

        const Body = fs.createReadStream(srcPath);
        const uploader = new Upload({
            client: this.s3,
            params: { Bucket: bucket, Key: key, Body, ContentType: contentType }
        });

        await uploader.done();

        const st = await fsp.stat(srcPath).catch(() => null);
        return st?.size ?? null;
    }
}

function parseS3Url(url) {
    // s3://bucket/optional/prefix/or/key
    if (!url || !url.startsWith('s3://')) {
        throw new Error(`Invalid S3 URL: ${url}`);
    }
    const without = url.slice('s3://'.length);
    const slash = without.indexOf('/');
    if (slash < 0) return { bucket: without, key: '', prefix: '' };

    const bucket = without.slice(0, slash);
    const rest = without.slice(slash + 1);
    const isPrefix = url.endsWith('/');

    return { bucket, key: isPrefix ? '' : rest, prefix: isPrefix ? rest : '' };
}

module.exports = { S3Adapter, parseS3Url, relPathForKey };
