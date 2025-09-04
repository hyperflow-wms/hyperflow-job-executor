'use strict';

const path = require('node:path');
const fs = require('node:fs');
const fsp = fs.promises;

const { S3Adapter, parseS3Url, relPathForKey } = require('./storage/s3Adapter');

function bool(value, defaultValue = false) {
    if (value === undefined || value === null) return defaultValue;
    if (typeof value === 'boolean') return value;
    const s = String(value).toLowerCase();
    return s === '1' || s === 'true' || s === 'yes';
}

function int(value, defaultValue) {
    const n = Number.parseInt(value, 10);
    return Number.isFinite(n) ? n : defaultValue;
}

function inferContentType(fileName) {
    const ext = path.extname(fileName).toLowerCase();
    if (ext === '.txt') return 'text/plain';
    if (ext === '.json') return 'application/json';
    if (ext === '.csv') return 'text/csv';
    if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
    if (ext === '.png') return 'image/png';
    // Let S3 set default if unknown
    return undefined;
}

async function walkDir(dir) {
    const acc = [];
    async function rec(current) {
        const entries = await fsp.readdir(current, { withFileTypes: true });
        for (const e of entries) {
            const p = path.join(current, e.name);
            if (e.isDirectory()) {
                await rec(p);
            } else {
                acc.push(p);
            }
        }
    }
    await rec(dir);
    return acc;
}

function sumBytes(arr, field = 'bytes') {
    return arr.reduce((a, x) => a + (typeof x[field] === 'number' ? x[field] : 0), 0);
}

/**
 * Downloads input objects from S3 to the local input directory, based on jm.io.inputs.
 * Supports both single keys and prefixes (with include/exclude filters).
 */
async function preRunDownload(jm, { inputDir, logger = console }) {
    const t0 = Date.now();

    if (!jm || !jm.io || !Array.isArray(jm.io.inputs) || jm.io.inputs.length === 0) {
        logger.info('Pre-run download: no inputs defined', JSON.stringify({ inputDir }));
        return { downloads: [] };
    }

    const s3 = new S3Adapter({ logger });
    const concurrency = int(process.env.HF_S3_CONCURRENCY, 6);
    logger.info('Pre-run download: start', JSON.stringify({ inputs: jm.io.inputs.length, concurrency, inputDir }));

    const tasks = [];

    for (const src of jm.io.inputs) {
        // Prefer canonical url format; fallback to {bucket, key}/{bucket, prefix}
        let parsed = null;
        if (src.url) {
            parsed = parseS3Url(src.url);
        } else {
            parsed = { bucket: src.bucket, key: src.key || '', prefix: src.prefix || '' };
        }

        const bucket = parsed.bucket;
        const key = parsed.key;
        const prefix = parsed.prefix;

        if (key) {
            const rel = path.basename(key);
            const dest = path.join(inputDir, rel);
            tasks.push({ bucket, key, prefix: '', dest, rel });
        } else if (prefix) {
            const listed = await s3.list({
                bucket,
                prefix,
                recursive: src.recursive !== false,
                include: Array.isArray(src.include) ? src.include : [],
                exclude: Array.isArray(src.exclude) ? src.exclude : [],
                maxFiles: src.maxFiles
            });

            if (listed.length === 0) {
                logger.warn('S3 list returned 0 objects for input', JSON.stringify({ bucket, prefix }));
            }

            for (const obj of listed) {
                const rel = relPathForKey(prefix, obj.key);
                const dest = path.join(inputDir, rel);
                tasks.push({ bucket, key: obj.key, prefix, dest, rel });
            }
        } else if (src.bucket && src.key) {
            const rel = path.basename(src.key);
            const dest = path.join(inputDir, rel);
            tasks.push({ bucket: src.bucket, key: src.key, prefix: '', dest, rel });
        } else {
            logger.warn('Input definition incomplete; skipping', JSON.stringify({ src }));
        }
    }

    if (tasks.length === 0) {
        logger.info('Pre-run download: no tasks to execute', JSON.stringify({ inputDir }));
        return { downloads: [] };
    }

    // Concurrency-limited downloader without external deps
    const queue = tasks.slice();
    const downloads = [];
    let active = 0;

    await new Promise((resolve, reject) => {
        let aborted = false;

        const next = async () => {
            if (aborted) return;

            const item = queue.shift();
            if (!item) {
                if (active === 0) resolve();
                return;
            }

            active += 1;
            try {
                fs.mkdirSync(path.dirname(item.dest), { recursive: true });
                const bytes = await s3.downloadToPath({
                    bucket: item.bucket,
                    key: item.key,
                    destPath: item.dest
                });

                logger.info('S3 download ok', JSON.stringify({ key: item.key, bytes, dest: item.dest }));
                downloads.push({ ...item, bytes });
            } catch (err) {
                aborted = true;
                logger.error('S3 download failed', JSON.stringify({ key: item.key, dest: item.dest, err: String(err) }));
                reject(err);
                return;
            } finally {
                active -= 1;
                next();
            }
        };

        const workers = Math.max(1, Math.min(concurrency, queue.length || 1));
        for (let i = 0; i < workers; i += 1) next();
    });

    if (!jm._wpok) jm._wpok = {};
    jm._wpok.downloads = downloads;

    logger.info(
        'Pre-run download: done',
        JSON.stringify({ count: downloads.length, totalBytes: sumBytes(downloads), durationMs: Date.now() - t0 })
    );

    return { downloads };
}

/**
 * Uploads output files from the local output directory to the target S3 prefix,
 * respecting overwrite/layout rules.
 */
async function postRunUpload(jm, { inputDir, outputDir, logger = console }) {
    const t0 = Date.now();

    if (!jm || !jm.io || !jm.io.output || !jm.io.output.url) {
        logger.info('Post-run upload: no output target defined', JSON.stringify({ outputDir }));
        return { uploads: [] };
    }

    const outParsed = parseS3Url(jm.io.output.url);
    const bucket = outParsed.bucket;
    const prefix = outParsed.prefix;
    const overwrite = jm.io.output.overwrite === true;
    const layout = jm.io.output.layout;

    const s3 = new S3Adapter({ logger });
    const concurrency = int(process.env.HF_S3_CONCURRENCY, 6);
    const outputs = Array.isArray(jm.outputs) ? jm.outputs : [];

    // Determine stem for layout when exactly one input is present
    let stem = null;
    const downloaded = (jm._wpok && Array.isArray(jm._wpok.downloads)) ? jm._wpok.downloads : [];

    if (downloaded.length === 1) {
        const base = path.basename(downloaded[0].key);
        stem = path.parse(base).name;
    } else if (Array.isArray(jm.inputs) && jm.inputs.length === 1) {
        stem = path.parse(jm.inputs[0].name).name;
    }

    if (layout && !stem) {
        logger.warn('Output layout provided but no single stem could be inferred; using original filenames', JSON.stringify({ layout }));
    }

    // Decide which files to upload
    let filesToUpload = [];

    if (outputs.length > 0) {
        filesToUpload = outputs.map(o => path.join(outputDir, o.name));
    } else {
        filesToUpload = await walkDir(outputDir);
    }

    if (filesToUpload.length === 0) {
        logger.warn('Post-run upload: no files found to upload', JSON.stringify({ outputDir }));
    }

    logger.info(
        'Post-run upload: start',
        JSON.stringify({ bucket, prefix, overwrite, layout: !!layout, files: filesToUpload.length, concurrency })
    );

    // Build upload tasks
    const tasks = filesToUpload.map(localPath => {
        const base = path.basename(localPath);
        const extless = path.parse(base).name;

        let keyName = base;
        if (layout && stem) keyName = layout.replace('{stem}', stem);

        const relFromOut = path.relative(outputDir, localPath);
        let finalKey = null;

        if (layout && stem) {
            finalKey = path.posix.join(prefix || '', keyName);
        } else {
            // Preserve relative folder structure under output_dir
            finalKey = path.posix.join(prefix || '', relFromOut.split(path.sep).join('/'));
        }

        return { bucket, key: finalKey, srcPath: localPath };
    });

    // Concurrency-limited uploader without external deps
    const queue = tasks.slice();
    const uploads = [];
    let active = 0;

    await new Promise((resolve, reject) => {
        let aborted = false;

        const next = async () => {
            if (aborted) return;

            const item = queue.shift();
            if (!item) {
                if (active === 0) resolve();
                return;
            }

            active += 1;
            try {
                const bytes = await s3.uploadFromPath({
                    bucket: item.bucket,
                    key: item.key,
                    srcPath: item.srcPath,
                    overwrite,
                    contentType: inferContentType(item.srcPath)
                });

                logger.info('S3 upload ok', JSON.stringify({ key: item.key, bytes, src: item.srcPath }));
                uploads.push({ ...item, bytes });
            } catch (err) {
                aborted = true;
                logger.error('S3 upload failed', JSON.stringify({ key: item.key, src: item.srcPath, err: String(err) }));
                reject(err);
                return;
            } finally {
                active -= 1;
                next();
            }
        };

        const workers = Math.max(1, Math.min(concurrency, queue.length || 1));
        for (let i = 0; i < workers; i += 1) next();
    });

    // Optional local cleanup
    if (bool(process.env.HF_TASK_CLEANUP_LOCAL, false)) {
        const unlinkInputs = (downloaded || []).map(d => fsp.unlink(d.dest).catch(() => {}));
        const unlinkOutputs = filesToUpload.map(p => fsp.unlink(p).catch(() => {}));
        await Promise.allSettled(unlinkInputs);
        await Promise.allSettled(unlinkOutputs);
        logger.info(
            'Local files cleanup done',
            JSON.stringify({ removedInputs: unlinkInputs.length, removedOutputs: unlinkOutputs.length })
        );
    }

    logger.info(
        'Post-run upload: done',
        JSON.stringify({ count: uploads.length, totalBytes: sumBytes(uploads), durationMs: Date.now() - t0 })
    );

    return { uploads };
}

module.exports = {
    preRunDownload,
    postRunUpload
};
