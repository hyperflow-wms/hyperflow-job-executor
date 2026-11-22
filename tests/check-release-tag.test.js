/**
 * Tests for the steps of CI's publish job that take the released version from
 * the release tag.
 *
 * Covers:
 *  1. scripts/check-release-tag.js accepts "v" followed by a semantic version
 *     and returns the version without the "v".
 *  2. It rejects everything else, including tags npm version would accept:
 *     no "v", a doubled "v", npm version keywords after the "v", partial or
 *     padded versions.
 *  3. Its command line exits 0 on a valid tag and 1 otherwise.
 *  4. The "Set the version from the release tag" step of
 *     .github/workflows/ci.yml, run as written on a copy of package.json and
 *     package-lock.json, writes the tag's version into both files.
 *  5. The same step fails on an invalid tag and leaves both files unchanged.
 *  6. The "Publish" step, run as written with a fake npm, publishes only on a
 *     release event: it passes no --tag for a release version and --tag next
 *     for a prerelease. Any other run, such as a manual run, only runs
 *     npm publish --dry-run, and a rehearsal of an already published version
 *     passes.
 *
 * Needs node, npm and bash; no network. Step 4 runs the real npm version.
 * Run with:  node tests/check-release-tag.test.js
 */
'use strict';
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawnSync } = require('child_process');
const { releaseVersion } = require('../scripts/check-release-tag.js');

const REPO = path.join(__dirname, '..');
const WORKFLOW = path.join(REPO, '.github', 'workflows', 'ci.yml');
const SCRIPT = path.join(REPO, 'scripts', 'check-release-tag.js');

/**
 * Returns the shell script of the step called `stepName` in ci.yml, dedented.
 * Expects the step to use a literal block: `run: |`.
 */
function workflowStepScript(stepName) {
    const lines = fs.readFileSync(WORKFLOW, 'utf8').split('\n');
    const indentOf = (line) => line.search(/\S/);
    const start = lines.findIndex((line) => line.trim() === '- name: ' + stepName);
    assert.notStrictEqual(start, -1, 'ci.yml has no step named "' + stepName + '"');
    const stepIndent = indentOf(lines[start]);
    let i = start + 1;
    while (i < lines.length && lines[i].trim() !== 'run: |'
           && (indentOf(lines[i]) === -1 || indentOf(lines[i]) > stepIndent)) {
        i++;
    }
    assert(i < lines.length && lines[i].trim() === 'run: |',
        'step "' + stepName + '" has no "run: |" block');
    const runIndent = indentOf(lines[i]);
    const body = [];
    for (i++; i < lines.length; i++) {
        if (indentOf(lines[i]) !== -1 && indentOf(lines[i]) <= runIndent) {
            break;
        }
        body.push(lines[i]);
    }
    const margin = Math.min(...body.filter((l) => l.trim()).map(indentOf));
    return body.map((l) => l.slice(margin)).join('\n').trim() + '\n';
}

/**
 * The environment a step starts from: this process's environment without the
 * GITHUB_* variables, so that running these tests inside GitHub Actions does
 * not hand the steps the test job's own event, ref or commit.
 */
function baseEnv() {
    const env = {};
    for (const [name, value] of Object.entries(process.env)) {
        if (!name.startsWith('GITHUB_')) {
            env[name] = value;
        }
    }
    return env;
}

/** Runs a step script the way GitHub Actions runs `run:` on Linux: bash -e. */
function runStep(script, cwd, env) {
    return spawnSync('bash', ['-e', '-c', script], {
        cwd,
        env: Object.assign(baseEnv(), env),
        encoding: 'utf8',
    });
}

function readVersions(dir) {
    const pkg = JSON.parse(fs.readFileSync(path.join(dir, 'package.json'), 'utf8'));
    const lock = JSON.parse(fs.readFileSync(path.join(dir, 'package-lock.json'), 'utf8'));
    return [pkg.version, lock.version, lock.packages[''].version];
}

/** A scratch checkout holding what the version step touches. */
function makeCheckout(root) {
    const dir = fs.mkdtempSync(path.join(root, 'checkout-'));
    fs.copyFileSync(path.join(REPO, 'package.json'), path.join(dir, 'package.json'));
    fs.copyFileSync(path.join(REPO, 'package-lock.json'), path.join(dir, 'package-lock.json'));
    fs.mkdirSync(path.join(dir, 'scripts'));
    fs.copyFileSync(SCRIPT, path.join(dir, 'scripts', 'check-release-tag.js'));
    return dir;
}

const VALID = [
    ['v1.4.3', '1.4.3'],
    ['v0.0.0', '0.0.0'],
    ['v10.20.30', '10.20.30'],
    ['v1.5.0-rc.1', '1.5.0-rc.1'],
    ['v1.0.0-alpha', '1.0.0-alpha'],
    ['v1.0.0-0.3.7', '1.0.0-0.3.7'],
    ['v1.0.0-x-y-z.--', '1.0.0-x-y-z.--'],
    ['v1.0.0-alpha+001', '1.0.0-alpha+001'],
    ['v1.0.0+20130313144700', '1.0.0+20130313144700'],
    ['v1.0.0-beta+exp.sha.5114f85', '1.0.0-beta+exp.sha.5114f85'],
];

const INVALID = [
    '1.4.3',            // no "v"
    'vv1.4.3',          // doubled "v": npm version would strip it
    'V1.4.3',
    'v=1.4.3',          // npm version strips "="
    'v 1.4.3',
    ' v1.4.3',
    'v1.4.3 ',
    'v1.4.3\n',
    'vmajor', 'vminor', 'vpatch', 'vpremajor', 'vpreminor', 'vprepatch',
    'vprerelease', 'vfrom-git',
    'v1.4', 'v1', 'v', '', 'v1.4.3.4',
    'v01.4.3', 'v1.04.3', 'v1.4.03',  // leading zeros
    'v1.4.3-01',        // numeric pre-release identifier with a leading zero
    'v1.4.3-', 'v1.4.3+', 'v1.4.3-rc..1', 'v1.4.3-rc.1+',
    'v1.4.3-rc_1',
    'v1.4.x', 'v^1.4.3', 'v1.4.3 || 2.0.0',
    'release-1.4.3',
];

function testAcceptsValidTags() {
    for (const [tag, version] of VALID) {
        assert.strictEqual(releaseVersion(tag), version,
            'expected ' + JSON.stringify(tag) + ' to give ' + version);
    }
    console.log('ok 1 - check-release-tag accepts "v" + semver and returns the version (' + VALID.length + ' tags)');
}

function testRejectsInvalidTags() {
    for (const tag of INVALID) {
        assert.strictEqual(releaseVersion(tag), null,
            'expected ' + JSON.stringify(tag) + ' to be rejected');
    }
    assert.strictEqual(releaseVersion(undefined), null);
    console.log('ok 2 - check-release-tag rejects tags that are not "v" + semver (' + INVALID.length + ' tags)');
}

function testCommandLine() {
    const run = (args) => spawnSync(process.execPath, [SCRIPT].concat(args), { encoding: 'utf8' });

    const ok = run(['v1.4.3']);
    assert.strictEqual(ok.status, 0, 'valid tag should exit 0: ' + ok.stderr);
    assert.match(ok.stdout, /gives version 1\.4\.3$/m);

    for (const args of [['1.4.3'], ['vmajor'], ['v1.4'], [], ['v1.4.3', 'v1.4.4']]) {
        const bad = run(args);
        assert.strictEqual(bad.status, 1,
            JSON.stringify(args) + ' should exit 1, got ' + bad.status + ': ' + bad.stdout + bad.stderr);
        assert.notStrictEqual(bad.stderr, '', JSON.stringify(args) + ' should explain the failure on stderr');
    }
    console.log('ok 3 - check-release-tag exits 0 on a valid tag and 1 on an invalid tag or bad usage');
}

function testVersionStepWritesTagVersion(root, script, npmEnv) {
    for (const [tag, version] of [['v1.4.3', '1.4.3'], ['v1.5.0-rc.1', '1.5.0-rc.1']]) {
        const dir = makeCheckout(root);
        const res = runStep(script, dir, Object.assign({ GITHUB_REF_NAME: tag }, npmEnv));
        assert.strictEqual(res.status, 0,
            'version step failed for ' + tag + ':\n' + res.stdout + res.stderr);
        assert.deepStrictEqual(readVersions(dir), [version, version, version],
            'package.json and package-lock.json should hold ' + version + ' after tag ' + tag);
    }
    console.log('ok 4 - the version step writes the tag\'s version into package.json and package-lock.json');
}

function testVersionStepRejectsBadTags(root, script, npmEnv) {
    for (const tag of ['1.4.3', 'v1.4', 'vmajor', 'vv1.4.3', 'vprerelease']) {
        const dir = makeCheckout(root);
        const before = [
            fs.readFileSync(path.join(dir, 'package.json'), 'utf8'),
            fs.readFileSync(path.join(dir, 'package-lock.json'), 'utf8'),
        ];
        const res = runStep(script, dir, Object.assign({ GITHUB_REF_NAME: tag }, npmEnv));
        assert.notStrictEqual(res.status, 0, 'version step should fail for tag ' + JSON.stringify(tag));
        assert.match(res.stderr, /is not "v" followed by a semantic version/,
            'tag ' + JSON.stringify(tag) + ' should fail in the tag check:\n' + res.stdout + res.stderr);
        const after = [
            fs.readFileSync(path.join(dir, 'package.json'), 'utf8'),
            fs.readFileSync(path.join(dir, 'package-lock.json'), 'utf8'),
        ];
        assert.deepStrictEqual(after, before, 'tag ' + JSON.stringify(tag) + ' must leave both files unchanged');
    }
    console.log('ok 5 - the version step fails on a tag that is not "v" + semver and changes nothing');
}

function testPublishStep(root) {
    const script = workflowStepScript('Publish');
    const bin = fs.mkdtempSync(path.join(root, 'bin-'));
    const callsFile = path.join(root, 'npm-calls');
    // Records its arguments, prints FAKE_NPM_OUTPUT and exits with FAKE_NPM_EXIT.
    fs.writeFileSync(path.join(bin, 'npm'),
        '#!/bin/sh\n'
        + 'printf \'%s\\n\' "$*" >> "$NPM_CALLS"\n'
        + '[ -z "$FAKE_NPM_OUTPUT" ] || printf \'%s\\n\' "$FAKE_NPM_OUTPUT"\n'
        + 'exit "${FAKE_NPM_EXIT:-0}"\n',
        { mode: 0o755 });
    const PATH = [bin, path.dirname(process.execPath), process.env.PATH].join(path.delimiter);

    function publish(version, env) {
        const dir = fs.mkdtempSync(path.join(root, 'publish-'));
        fs.writeFileSync(path.join(dir, 'package.json'), JSON.stringify({ version }));
        fs.writeFileSync(callsFile, '');
        const res = runStep(script, dir, Object.assign(
            { PATH, NPM_CALLS: callsFile, FAKE_NPM_OUTPUT: '', FAKE_NPM_EXIT: '0' }, env));
        const calls = fs.readFileSync(callsFile, 'utf8').split('\n').filter(Boolean);
        return { status: res.status, output: res.stdout + res.stderr, calls };
    }
    const release = { GITHUB_EVENT_NAME: 'release' };
    const manual = { GITHUB_EVENT_NAME: 'workflow_dispatch' };

    let r = publish('1.4.3', release);
    assert.strictEqual(r.status, 0, r.output);
    assert.deepStrictEqual(r.calls, ['publish'], 'a release version takes npm\'s default dist-tag');

    r = publish('1.5.0-rc.1', release);
    assert.strictEqual(r.status, 0, r.output);
    assert.deepStrictEqual(r.calls, ['publish --tag next'], 'a prerelease goes out under "next"');

    r = publish('1.4.3+build-7', release);
    assert.deepStrictEqual(r.calls, ['publish'], 'a hyphen in build metadata does not make a prerelease');

    // A manual run never publishes, whatever the version, also when it is
    // dispatched on a tag; neither does a run with no event name.
    for (const [version, env, calls] of [
        ['0.0.0-dev', manual, ['publish --dry-run --tag next']],
        ['1.4.3', manual, ['publish --dry-run']],
        ['1.4.3', Object.assign({ GITHUB_REF_NAME: 'v1.4.3' }, manual), ['publish --dry-run']],
        ['1.5.0-rc.1', manual, ['publish --dry-run --tag next']],
        ['1.4.3', {}, ['publish --dry-run']],
    ]) {
        r = publish(version, env);
        assert.strictEqual(r.status, 0, r.output);
        assert.deepStrictEqual(r.calls, calls,
            'a run with ' + JSON.stringify(env) + ' must only rehearse ' + version);
    }

    r = publish('1.4.2', Object.assign({
        FAKE_NPM_EXIT: '1',
        FAKE_NPM_OUTPUT: 'npm error You cannot publish over the previously published versions: 1.4.2.',
    }, manual));
    assert.strictEqual(r.status, 0, 'a rehearsal of a published version should pass:\n' + r.output);
    assert.deepStrictEqual(r.calls, ['publish --dry-run']);

    r = publish('1.4.2', Object.assign({ FAKE_NPM_EXIT: '1', FAKE_NPM_OUTPUT: 'npm error code E403' }, manual));
    assert.strictEqual(r.status, 1, 'any other rehearsal failure should fail:\n' + r.output);

    console.log('ok 6 - the publish step publishes only on a release, picks the dist-tag and handles rehearsal outcomes');
}

function main() {
    testAcceptsValidTags();
    testRejectsInvalidTags();
    testCommandLine();

    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'check-release-tag-'));
    // Keep npm off the user's cache and off the network.
    const npmEnv = {
        npm_config_cache: path.join(root, 'npm-cache'),
        npm_config_update_notifier: 'false',
        npm_config_fund: 'false',
        npm_config_audit: 'false',
    };
    try {
        const script = workflowStepScript('Set the version from the release tag');
        testVersionStepWritesTagVersion(root, script, npmEnv);
        testVersionStepRejectsBadTags(root, script, npmEnv);
        testPublishStep(root);
    } finally {
        fs.rmSync(root, { recursive: true, force: true });
    }
    console.log('all check-release-tag.test.js tests passed');
}

try {
    main();
} catch (err) {
    console.error(err);
    process.exit(1);
}
