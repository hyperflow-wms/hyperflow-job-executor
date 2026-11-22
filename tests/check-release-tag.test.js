/**
 * Tests for the steps of CI's publish job that take the released version from
 * the release tag.
 *
 * Covers:
 *  1. scripts/check-release-tag.js accepts "v" followed by a semantic version
 *     and returns the version without the "v".
 *  2. It rejects everything else, including tags npm version would accept:
 *     no "v", a doubled "v", npm version keywords after the "v", partial or
 *     padded versions, and versions with build metadata, which npm drops.
 *  3. Its command line exits 0 on a valid tag and 1 otherwise. On a valid tag
 *     it prints only the version on stdout and, when GITHUB_ENV is set,
 *     appends RELEASE_VERSION=<version> to that file once.
 *  4. The "Check the release tag" and "Set the version from the release tag"
 *     steps of .github/workflows/ci.yml, run as written one after the other
 *     on a copy of package.json and package-lock.json, write the tag's version
 *     into both files. The version reaches the second step through GITHUB_ENV,
 *     which the test handles the way GitHub Actions does; the version step
 *     writes RELEASE_VERSION, not the version in the tag.
 *  5. On an invalid tag, including one with build metadata, the tag check
 *     fails, the job stops there, and both files stay unchanged.
 *  6. The "Publish" step, run as written with a fake npm, publishes only on a
 *     release event: it passes no --tag for a release version and --tag next
 *     for a prerelease. Any other run, such as a manual run, only runs
 *     npm publish --dry-run, and a rehearsal of an already published version
 *     passes.
 *  7. The "Check the released commit is on master" step, run as written in a
 *     shallow checkout of a tag like the one actions/checkout makes on a
 *     release, passes for a commit on master and fails, with a message, for a
 *     commit that is not on master and when git cannot answer.
 *  8. The "Refuse to republish a version already on npm" step, run as written
 *     after the tag check with a fake npm that prints what npm 11 prints,
 *     passes only when npm view reports E404, and fails when the version is
 *     published or npm view fails in any other way. It asks npm about
 *     RELEASE_VERSION, not the version in the tag.
 *
 * Needs node, npm, bash and git; no network. Step 4 runs the real npm version,
 * step 7 the real git against a local repository.
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
 * GITHUB_* variables and RELEASE_VERSION, which the steps get only from the
 * tests. Otherwise running these tests inside GitHub Actions would hand the
 * steps the test job's own event, ref or commit, and a RELEASE_VERSION set in
 * the shell would reach steps that must run without one.
 */
function baseEnv() {
    const env = {};
    for (const [name, value] of Object.entries(process.env)) {
        if (!name.startsWith('GITHUB_') && name !== 'RELEASE_VERSION') {
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

/**
 * Runs the named steps of ci.yml one after another, the way GitHub Actions
 * runs the steps of a job. Each step gets its own new, empty file named by
 * GITHUB_ENV. After the step ends, the NAME=value lines it appended there
 * become environment variables of the steps after it, not of the step itself.
 * Stops at the first step that fails, as a job does. Returns, for each step
 * that ran, its exit status, output and what it wrote to GITHUB_ENV.
 */
function runSteps(root, stepNames, cwd, env) {
    const runnerDir = fs.mkdtempSync(path.join(root, 'runner-'));
    const jobEnv = Object.assign({}, env);
    const results = [];
    stepNames.forEach((name, i) => {
        if (results.length > 0 && results[results.length - 1].status !== 0) {
            return;
        }
        const envFile = path.join(runnerDir, 'set_env_' + i);
        fs.writeFileSync(envFile, '');
        const res = runStep(workflowStepScript(name), cwd, Object.assign({}, jobEnv, { GITHUB_ENV: envFile }));
        const written = fs.readFileSync(envFile, 'utf8');
        results.push({ name, status: res.status, stdout: res.stdout, stderr: res.stderr, githubEnv: written });
        if (res.status !== 0) {
            return;
        }
        for (const line of written.split('\n').filter(Boolean)) {
            const m = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(line);
            assert(m, 'step "' + name + '" wrote a line to GITHUB_ENV that is not NAME=value: ' + JSON.stringify(line));
            jobEnv[m[1]] = m[2];
        }
    });
    return results;
}

/**
 * Puts a fake npm in a new directory. It appends its arguments to $NPM_CALLS,
 * prints $FAKE_NPM_OUTPUT on stdout and $FAKE_NPM_STDERR on stderr, and exits
 * with $FAKE_NPM_EXIT. Returns the environment that puts it first on PATH and
 * a function that returns the calls recorded so far and clears them.
 */
function fakeNpm(root) {
    const bin = fs.mkdtempSync(path.join(root, 'bin-'));
    const callsFile = path.join(bin, 'calls');
    fs.writeFileSync(path.join(bin, 'npm'),
        '#!/bin/sh\n'
        + 'printf \'%s\\n\' "$*" >> "$NPM_CALLS"\n'
        + '[ -z "$FAKE_NPM_OUTPUT" ] || printf \'%s\\n\' "$FAKE_NPM_OUTPUT"\n'
        + '[ -z "$FAKE_NPM_STDERR" ] || printf \'%s\\n\' "$FAKE_NPM_STDERR" >&2\n'
        + 'exit "${FAKE_NPM_EXIT:-0}"\n',
        { mode: 0o755 });
    fs.writeFileSync(callsFile, '');
    return {
        env: {
            PATH: [bin, path.dirname(process.execPath), process.env.PATH].join(path.delimiter),
            NPM_CALLS: callsFile,
            FAKE_NPM_OUTPUT: '',
            FAKE_NPM_STDERR: '',
            FAKE_NPM_EXIT: '0',
        },
        takeCalls() {
            const calls = fs.readFileSync(callsFile, 'utf8').split('\n').filter(Boolean);
            fs.writeFileSync(callsFile, '');
            return calls;
        },
    };
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
];

// Valid semver, but npm version drops the build metadata, so each would
// publish a version other than the one the tag names.
const BUILD_METADATA = [
    'v1.4.3+build.7',
    'v1.0.0-alpha+001',
    'v1.0.0+20130313144700',
    'v1.0.0-beta+exp.sha.5114f85',
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
].concat(BUILD_METADATA);

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
    console.log('ok 2 - check-release-tag rejects tags that are not "v" + semver or carry build metadata ('
        + INVALID.length + ' tags)');
}

function testCommandLine(root) {
    const run = (args, env) => spawnSync(process.execPath, [SCRIPT].concat(args),
        { encoding: 'utf8', env: Object.assign(baseEnv(), env) });

    // Outside GitHub Actions: only the version on stdout, no file written.
    let ok = run(['v1.4.3']);
    assert.strictEqual(ok.status, 0, 'valid tag should exit 0: ' + ok.stderr);
    assert.strictEqual(ok.stdout, '1.4.3\n', 'stdout should hold the version and nothing else');
    assert.match(ok.stderr, /gives version 1\.4\.3/);

    // In GitHub Actions: one line appended after what the file holds.
    const envFile = path.join(root, 'github-env');
    fs.writeFileSync(envFile, 'EARLIER=1\n');
    ok = run(['v1.5.0-rc.1'], { GITHUB_ENV: envFile });
    assert.strictEqual(ok.status, 0, 'valid tag should exit 0: ' + ok.stderr);
    assert.strictEqual(ok.stdout, '1.5.0-rc.1\n', 'stdout should hold the version and nothing else');
    assert.strictEqual(fs.readFileSync(envFile, 'utf8'), 'EARLIER=1\nRELEASE_VERSION=1.5.0-rc.1\n',
        'GITHUB_ENV should gain exactly one RELEASE_VERSION line');

    for (const args of [['1.4.3'], ['vmajor'], ['v1.4'], [], ['v1.4.3', 'v1.4.4'], ['v1.4.3+build.7']]) {
        fs.writeFileSync(envFile, '');
        const bad = run(args, { GITHUB_ENV: envFile });
        assert.strictEqual(bad.status, 1,
            JSON.stringify(args) + ' should exit 1, got ' + bad.status + ': ' + bad.stdout + bad.stderr);
        assert.notStrictEqual(bad.stderr, '', JSON.stringify(args) + ' should explain the failure on stderr');
        assert.strictEqual(bad.stdout, '', JSON.stringify(args) + ' should print nothing on stdout');
        assert.strictEqual(fs.readFileSync(envFile, 'utf8'), '', JSON.stringify(args) + ' should not touch GITHUB_ENV');
    }

    const build = run(['v1.4.3+build.7']);
    assert.match(build.stderr, /carries build metadata \("\+build\.7"\), which npm drops, so the package would be published as 1\.4\.3\./);
    console.log('ok 3 - check-release-tag prints only the version and sets RELEASE_VERSION on a valid tag,'
        + ' and exits 1 on an invalid tag or bad usage');
}

const VERSION_STEPS = ['Check the release tag', 'Set the version from the release tag'];

function testVersionStepWritesTagVersion(root, npmEnv) {
    for (const [tag, version] of [['v1.4.3', '1.4.3'], ['v1.5.0-rc.1', '1.5.0-rc.1']]) {
        const dir = makeCheckout(root);
        const results = runSteps(root, VERSION_STEPS, dir,
            Object.assign({ GITHUB_EVENT_NAME: 'release', GITHUB_REF_NAME: tag }, npmEnv));
        for (const r of results) {
            assert.strictEqual(r.status, 0, 'step "' + r.name + '" failed for ' + tag + ':\n' + r.stdout + r.stderr);
        }
        assert.strictEqual(results.length, 2);
        assert.strictEqual(results[0].githubEnv, 'RELEASE_VERSION=' + version + '\n');
        assert.deepStrictEqual(readVersions(dir), [version, version, version],
            'package.json and package-lock.json should hold ' + version + ' after tag ' + tag);
    }

    // The version step takes the version from the tag check, not from the tag:
    // without the tag check before it, it fails and changes nothing.
    const dir = makeCheckout(root);
    const before = readVersions(dir);
    const [alone] = runSteps(root, ['Set the version from the release tag'], dir,
        Object.assign({ GITHUB_EVENT_NAME: 'release', GITHUB_REF_NAME: 'v1.4.3' }, npmEnv));
    assert.notStrictEqual(alone.status, 0, 'the version step should fail without RELEASE_VERSION');
    assert.match(alone.stderr, /RELEASE_VERSION: is not set/);
    assert.deepStrictEqual(readVersions(dir), before);

    // Given RELEASE_VERSION, it writes that version, whatever the tag says.
    const other = makeCheckout(root);
    const [given] = runSteps(root, ['Set the version from the release tag'], other, Object.assign(
        { GITHUB_EVENT_NAME: 'release', GITHUB_REF_NAME: 'v9.9.9', RELEASE_VERSION: '1.4.3' }, npmEnv));
    assert.strictEqual(given.status, 0, 'the version step failed:\n' + given.stdout + given.stderr);
    assert.deepStrictEqual(readVersions(other), ['1.4.3', '1.4.3', '1.4.3'],
        'the version step should write RELEASE_VERSION, not the version in GITHUB_REF_NAME');

    console.log('ok 4 - the tag check and the version step write the tag\'s version into package.json'
        + ' and package-lock.json');
}

function testVersionStepRejectsBadTags(root, npmEnv) {
    const tags = [
        ['1.4.3', /is not "v" followed by a semantic version/],
        ['v1.4', /is not "v" followed by a semantic version/],
        ['vmajor', /is not "v" followed by a semantic version/],
        ['vv1.4.3', /is not "v" followed by a semantic version/],
        ['vprerelease', /is not "v" followed by a semantic version/],
        ['v1.4.3+build.7', /carries build metadata/],
    ];
    for (const [tag, message] of tags) {
        const dir = makeCheckout(root);
        const before = [
            fs.readFileSync(path.join(dir, 'package.json'), 'utf8'),
            fs.readFileSync(path.join(dir, 'package-lock.json'), 'utf8'),
        ];
        const results = runSteps(root, VERSION_STEPS, dir,
            Object.assign({ GITHUB_EVENT_NAME: 'release', GITHUB_REF_NAME: tag }, npmEnv));
        assert.strictEqual(results.length, 1, 'the job should stop at the tag check for ' + JSON.stringify(tag));
        const res = results[0];
        assert.notStrictEqual(res.status, 0, 'the tag check should fail for tag ' + JSON.stringify(tag));
        assert.match(res.stderr, message,
            'tag ' + JSON.stringify(tag) + ' should fail in the tag check:\n' + res.stdout + res.stderr);
        assert.strictEqual(res.githubEnv, '', 'tag ' + JSON.stringify(tag) + ' must not set RELEASE_VERSION');
        const after = [
            fs.readFileSync(path.join(dir, 'package.json'), 'utf8'),
            fs.readFileSync(path.join(dir, 'package-lock.json'), 'utf8'),
        ];
        assert.deepStrictEqual(after, before, 'tag ' + JSON.stringify(tag) + ' must leave both files unchanged');
    }
    console.log('ok 5 - the tag check fails on a tag that is not "v" + semver or carries build metadata,'
        + ' and nothing changes');
}

function testPublishStep(root) {
    const script = workflowStepScript('Publish');
    const npm = fakeNpm(root);

    function publish(version, env) {
        const dir = fs.mkdtempSync(path.join(root, 'publish-'));
        fs.writeFileSync(path.join(dir, 'package.json'), JSON.stringify({ version }));
        const res = runStep(script, dir, Object.assign({}, npm.env, env));
        return { status: res.status, output: res.stdout + res.stderr, calls: npm.takeCalls() };
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

/** Runs git and returns its trimmed stdout; throws if git fails. */
function git(args, cwd, env) {
    const res = spawnSync('git', args, { cwd, env: Object.assign(baseEnv(), env), encoding: 'utf8' });
    if (res.error) {
        throw res.error;
    }
    assert.strictEqual(res.status, 0, 'git ' + args.join(' ') + ' failed:\n' + res.stdout + res.stderr);
    return res.stdout.trim();
}

function testMasterCheckStep(root) {
    const script = workflowStepScript('Check the released commit is on master');
    // Keep git off the user's configuration and give it an identity to commit
    // with.
    const gitEnv = {
        GIT_CONFIG_GLOBAL: os.devNull,
        GIT_CONFIG_NOSYSTEM: '1',
        GIT_AUTHOR_NAME: 'Release Test',
        GIT_AUTHOR_EMAIL: 'release-test@example.invalid',
        GIT_COMMITTER_NAME: 'Release Test',
        GIT_COMMITTER_EMAIL: 'release-test@example.invalid',
        GIT_TERMINAL_PROMPT: '0',
    };

    // The origin repository:
    //   m1 - m2 - m3          master; v1.4.3 on m2, v1.4.4 on m3
    //          \
    //           s1            side; v1.5.0-rc.1 on s1, which is not on master
    const origin = path.join(root, 'origin');
    fs.mkdirSync(origin);
    git(['init', '-q'], origin, gitEnv);
    git(['symbolic-ref', 'HEAD', 'refs/heads/master'], origin, gitEnv);
    const commit = (message) => git(['commit', '-q', '--allow-empty', '-m', message], origin, gitEnv);
    commit('m1');
    commit('m2');
    git(['tag', 'v1.4.3'], origin, gitEnv);
    git(['checkout', '-q', '-b', 'side'], origin, gitEnv);
    commit('s1');
    git(['tag', 'v1.5.0-rc.1'], origin, gitEnv);
    git(['checkout', '-q', 'master'], origin, gitEnv);
    commit('m3');
    git(['tag', 'v1.4.4'], origin, gitEnv);

    // What actions/checkout@v4 does on a release with its default
    // fetch-depth of 1: the tag alone, without history and without branches.
    function checkout(tag) {
        const dir = fs.mkdtempSync(path.join(root, 'release-checkout-'));
        git(['init', '-q'], dir, gitEnv);
        git(['remote', 'add', 'origin', 'file://' + origin], dir, gitEnv);
        git(['-c', 'protocol.version=2', 'fetch', '-q', '--no-tags', '--prune', '--no-recurse-submodules',
            '--depth=1', 'origin', '+refs/tags/' + tag + ':refs/tags/' + tag], dir, gitEnv);
        git(['checkout', '-q', '--force', 'refs/tags/' + tag], dir, gitEnv);
        return { dir, sha: git(['rev-parse', 'HEAD'], dir, gitEnv) };
    }

    function check(co, tag, sha) {
        const res = runStep(script, co.dir, Object.assign(
            { GITHUB_EVENT_NAME: 'release', GITHUB_REF_NAME: tag, GITHUB_SHA: sha || co.sha }, gitEnv));
        return { status: res.status, output: res.stdout + res.stderr, stderr: res.stderr };
    }

    // The checkout is as shallow as the one on GitHub: without a fetch, git
    // cannot answer the question at all.
    let co = checkout('v1.4.3');
    assert.strictEqual(git(['rev-parse', '--is-shallow-repository'], co.dir, gitEnv), 'true');
    const bare = spawnSync('git', ['merge-base', '--is-ancestor', co.sha, 'origin/master'],
        { cwd: co.dir, env: Object.assign(baseEnv(), gitEnv), encoding: 'utf8' });
    assert.strictEqual(bare.status, 128, 'merge-base in the checkout before any fetch should exit 128');

    let r = check(co, 'v1.4.3');
    assert.strictEqual(r.status, 0, 'a commit on master that master has moved past should pass:\n' + r.output);

    co = checkout('v1.4.4');
    r = check(co, 'v1.4.4');
    assert.strictEqual(r.status, 0, 'the commit at the tip of master should pass:\n' + r.output);

    co = checkout('v1.5.0-rc.1');
    r = check(co, 'v1.5.0-rc.1');
    assert.strictEqual(r.status, 1, 'a commit that is not on master should fail:\n' + r.output);
    assert.match(r.stderr, /which is not on master/);

    // git merge-base exits 128 for a commit it does not know.
    co = checkout('v1.4.4');
    r = check(co, 'v1.4.4', 'deadbeef'.repeat(5));
    assert.strictEqual(r.status, 1, 'an error from git merge-base should fail the step:\n' + r.output);
    assert.match(r.stderr, /Could not check whether .* is on master: git merge-base exited with 128/);

    // An origin without master: the fetch fails.
    co = checkout('v1.4.4');
    git(['remote', 'set-url', 'origin', 'file://' + path.join(root, 'no-such-repository')], co.dir, gitEnv);
    r = check(co, 'v1.4.4');
    assert.strictEqual(r.status, 1, 'a failed fetch should fail the step:\n' + r.output);
    assert.match(r.stderr, /Could not fetch master from origin/);

    console.log('ok 7 - the master check passes for a commit on master and fails for one that is not,'
        + ' and when git cannot answer');
}

/**
 * What npm 11.19.1 prints for `npm view @hyperflow/job-executor@<version>
 * version --json`, as the fake npm's environment: on success the version as a
 * JSON string; on failure the error as JSON on stdout, messages on stderr and
 * exit code 1. The line naming npm's debug log is left out.
 */
const NPM_VIEW = {
    published: (version) => ({ FAKE_NPM_EXIT: '0', FAKE_NPM_OUTPUT: JSON.stringify(version) }),
    notFound: (version) => ({
        FAKE_NPM_EXIT: '1',
        FAKE_NPM_OUTPUT: JSON.stringify({ error: {
            code: 'E404',
            summary: 'No match found for version ' + version,
            detail: 'The requested resource \'@hyperflow/job-executor@' + version + '\' could not be found'
                + ' or you do not have permission to access it.\n\nNote that you can also install from a\n'
                + 'tarball, folder, http url, or git url.',
        } }, null, 2),
        FAKE_NPM_STDERR: [
            'npm error code E404',
            'npm error 404 No match found for version ' + version,
            'npm error 404',
            'npm error 404  The requested resource \'@hyperflow/job-executor@' + version + '\' could not be found'
                + ' or you do not have permission to access it.',
            'npm error 404',
            'npm error 404 Note that you can also install from a',
            'npm error 404 tarball, folder, http url, or git url.',
        ].join('\n'),
    }),
    connectionRefused: () => ({
        FAKE_NPM_EXIT: '1',
        FAKE_NPM_OUTPUT: JSON.stringify({ error: {
            code: 'ECONNREFUSED',
            summary: 'FetchError: request to https://registry.npmjs.org/@hyperflow%2fjob-executor failed,'
                + ' reason: connect ECONNREFUSED 127.0.0.1:443',
            detail: 'If you are behind a proxy, please make sure that the \'proxy\' config is set properly.'
                + '  See: \'npm help config\'',
        } }, null, 2),
        FAKE_NPM_STDERR: 'npm error code ECONNREFUSED\nnpm error syscall connect\nnpm error errno ECONNREFUSED',
    }),
};

function testRepublishStep(root) {
    const steps = ['Check the release tag', 'Refuse to republish a version already on npm'];
    const npm = fakeNpm(root);
    const dir = makeCheckout(root);

    function republishCheck(tag, npmView) {
        const results = runSteps(root, steps, dir, Object.assign(
            { GITHUB_EVENT_NAME: 'release', GITHUB_REF_NAME: tag }, npm.env, npmView));
        assert.strictEqual(results.length, 2, 'the tag check should pass for ' + tag + ':\n'
            + results.map((r) => r.stdout + r.stderr).join('\n'));
        const r = results[1];
        return { status: r.status, stdout: r.stdout, stderr: r.stderr, calls: npm.takeCalls() };
    }

    // E404: not published yet. The version comes from the tag check.
    for (const [tag, version] of [['v1.4.3', '1.4.3'], ['v1.5.0-rc.1', '1.5.0-rc.1']]) {
        const r = republishCheck(tag, NPM_VIEW.notFound(version));
        assert.strictEqual(r.status, 0, 'E404 should let the release go on:\n' + r.stdout + r.stderr);
        assert.deepStrictEqual(r.calls, ['view @hyperflow/job-executor@' + version + ' version --json']);
        assert.match(r.stdout, /is not on npm yet/);
    }

    let r = republishCheck('v1.4.2', NPM_VIEW.published('1.4.2'));
    assert.strictEqual(r.status, 1, 'a published version should fail the step:\n' + r.stdout + r.stderr);
    assert.match(r.stderr, /@hyperflow\/job-executor@1\.4\.2 is already published/);

    r = republishCheck('v1.4.3', NPM_VIEW.connectionRefused());
    assert.strictEqual(r.status, 1, 'an error other than E404 should fail the step:\n' + r.stdout + r.stderr);
    assert.match(r.stdout, /"code": "ECONNREFUSED"/, 'npm\'s JSON error should be shown');
    assert.match(r.stderr, /npm error code ECONNREFUSED/, 'npm\'s messages should be shown');
    assert.match(r.stderr, /an error other than E404/);

    // A failure without the JSON error on stdout is not taken for E404, even
    // when the messages on stderr mention it.
    r = republishCheck('v1.4.3', { FAKE_NPM_EXIT: '1', FAKE_NPM_OUTPUT: '', FAKE_NPM_STDERR: 'npm error code E404' });
    assert.strictEqual(r.status, 1, 'a failure without a JSON error should fail the step:\n' + r.stdout + r.stderr);

    // Given RELEASE_VERSION, it asks npm about that version, whatever the tag
    // says.
    const [given] = runSteps(root, [steps[1]], dir, Object.assign(
        { GITHUB_EVENT_NAME: 'release', GITHUB_REF_NAME: 'v9.9.9', RELEASE_VERSION: '1.4.3' },
        npm.env, NPM_VIEW.notFound('1.4.3')));
    assert.strictEqual(given.status, 0, 'E404 should let the release go on:\n' + given.stdout + given.stderr);
    assert.deepStrictEqual(npm.takeCalls(), ['view @hyperflow/job-executor@1.4.3 version --json'],
        'the republish check should ask about RELEASE_VERSION, not the version in GITHUB_REF_NAME');

    // Without the tag check before it, the step has no version and stops
    // before it runs npm.
    const [alone] = runSteps(root, [steps[1]], dir, Object.assign(
        { GITHUB_EVENT_NAME: 'release', GITHUB_REF_NAME: 'v1.4.3' }, npm.env, NPM_VIEW.notFound('1.4.3')));
    assert.notStrictEqual(alone.status, 0, 'the step should fail without RELEASE_VERSION');
    assert.deepStrictEqual(npm.takeCalls(), [], 'npm must not run without RELEASE_VERSION');

    console.log('ok 8 - the republish check passes only on E404 and fails when the version is published'
        + ' or npm view fails otherwise');
}

function main() {
    testAcceptsValidTags();
    testRejectsInvalidTags();

    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'check-release-tag-'));
    // Keep npm off the user's cache and off the network.
    const npmEnv = {
        npm_config_cache: path.join(root, 'npm-cache'),
        npm_config_update_notifier: 'false',
        npm_config_fund: 'false',
        npm_config_audit: 'false',
    };
    try {
        testCommandLine(root);
        testVersionStepWritesTagVersion(root, npmEnv);
        testVersionStepRejectsBadTags(root, npmEnv);
        testPublishStep(root);
        testMasterCheckStep(root);
        testRepublishStep(root);
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
