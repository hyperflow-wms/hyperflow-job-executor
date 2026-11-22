#!/usr/bin/env node
/**
 * Checks that a release tag is "v" followed by a version that follows the
 * Semantic Versioning 2.0.0 grammar (https://semver.org) and carries no build
 * metadata, for example v1.4.3 or v1.5.0-rc.1. Build metadata (a "+" part, as
 * in v1.4.3+build.7) is valid semver, but `npm version` drops it, so the
 * package would be published as a version other than the one the tag names.
 *
 * CI's publish job runs this on the release tag before it writes the tag's
 * version into package.json with `npm version`. The check has to come first:
 * `npm version` also accepts keywords such as "major" or "prerelease", so the
 * tag "vmajor" would bump the major version, and it strips a leading "v" or
 * "=" and surrounding whitespace, so "vv1.2.3" would pass as 1.2.3.
 *
 * Usage: node scripts/check-release-tag.js <tag>
 * Exits 0 and prints the version if the tag is valid, exits 1 and explains why
 * otherwise.
 *
 * Not part of the npm package: package.json's "files" list leaves it out.
 */
'use strict';

// The regular expression semver.org gives for the Semantic Versioning 2.0.0
// grammar. It requires MAJOR.MINOR.PATCH without leading zeros, and allows an
// optional pre-release part (dot-separated identifiers, numeric ones without
// leading zeros) and an optional build-metadata part, captured as group 5.
const SEMVER = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/;

/** Matches the tag against "v" + SEMVER; returns the match or null. */
function matchTag(tag) {
    if (typeof tag !== 'string' || !tag.startsWith('v')) {
        return null;
    }
    return SEMVER.exec(tag.slice(1));
}

/**
 * Returns the version a release tag names (the tag without its leading "v"),
 * or null if the tag is not "v" followed by a semantic version, or if the
 * version carries build metadata.
 */
function releaseVersion(tag) {
    const match = matchTag(tag);
    return match !== null && match[5] === undefined ? match[0] : null;
}

function main(argv) {
    if (argv.length !== 1) {
        console.error('usage: check-release-tag.js <tag>');
        return 1;
    }
    const tag = argv[0];
    const version = releaseVersion(tag);
    if (version === null) {
        const match = matchTag(tag);
        if (match !== null) {
            console.error('Release tag ' + JSON.stringify(tag) + ' carries build metadata ("+' + match[5] + '"),'
                + ' which npm drops, so the package would be published as '
                + match[0].slice(0, -(match[5].length + 1)) + '. Use a tag without build metadata.');
        } else {
            console.error('Release tag ' + JSON.stringify(tag) + ' is not "v" followed by a semantic version'
                + ' (for example v1.4.3 or v1.5.0-rc.1); see https://semver.org');
        }
        return 1;
    }
    console.log('Release tag ' + tag + ' gives version ' + version);
    return 0;
}

if (require.main === module) {
    process.exit(main(process.argv.slice(2)));
}

module.exports = { releaseVersion, main };
