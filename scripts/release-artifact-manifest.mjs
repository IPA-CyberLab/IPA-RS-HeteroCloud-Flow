import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

export const component = 'flow';
export const repository = 'ghcr.io/ipa-cyberlab/ipa-rs-heterocloud-flow';
export function validateRelease({ tag, commit, head, tagCommit }) {
  if (typeof tag !== 'string' || tag.length > 128 || !/^v?\d+\.\d+\.\d+(?:[.-][A-Za-z0-9._-]+)?$/.test(tag))
    throw new Error('Unsupported release tag');
  if (![commit, head, tagCommit].every(value => typeof value === 'string' && /^[a-f0-9]{40}$/.test(value)))
    throw new Error('Expected full lowercase commit SHA');
  if (commit !== head || commit !== tagCommit) throw new Error('Release event, HEAD and exact tag must match');
  return tag.replace(/^v/, '');
}
export function image(digest, repo = repository) {
  if (typeof digest !== 'string' || !/^sha256:[a-f0-9]{64}$/.test(digest)) throw new Error('Invalid pushed digest');
  return repo + '@' + digest;
}
export function createManifest(release, digest, companionDigest) {
  const artifact = { schema_version: 1, component, version: validateRelease(release), commit: release.commit, image: image(digest) };
  artifact.companions = { livekit: { image: image(companionDigest, repository + '-livekit') } };
  return artifact;
}
export function assertAssetsAbsent(assets, names) {
  if (!Array.isArray(assets) || !assets.every(asset => asset && typeof asset.name === 'string'))
    throw new Error('Invalid release asset listing');
  if (assets.some(asset => names.includes(asset.name))) throw new Error('Release asset already exists');
}
export function checkedOutRelease(env = process.env, cwd = process.cwd()) {
  const release = { tag: env.RELEASE_TAG, commit: env.GITHUB_SHA, head: env.GITHUB_SHA, tagCommit: env.GITHUB_SHA };
  validateRelease(release);
  const rev = ref => execFileSync('git', ['rev-parse', '--verify', ref], { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
  release.head = rev('HEAD^{commit}');
  release.tagCommit = rev('refs/tags/' + release.tag + '^{commit}');
  validateRelease(release);
  return release;
}
export function writeManifest(path, manifest) {
  writeFileSync(path, JSON.stringify(manifest, null, 2) + '\n', { flag: 'wx' });
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const [command, ...args] = process.argv.slice(2);
    const release = checkedOutRelease();
    if (command === 'validate' && args.length === 0) {
      process.stdout.write(validateRelease(release) + '\n');
    } else if (command === 'guard' && args.length > 0) {
      const response = JSON.parse(execFileSync('gh', ['release', 'view', release.tag, '--repo', process.env.GITHUB_REPOSITORY, '--json', 'assets'], { encoding: 'utf8' }));
      assertAssetsAbsent(response.assets, args);
    } else if (command === 'generate' && args.length === 1) {
      writeManifest(args[0], createManifest(release, process.env.IMAGE_DIGEST, process.env.LIVEKIT_DIGEST));
    } else if (command === 'digest-input' && args.length === 1) {
      process.stdout.write(image(readFileSync(args[0], 'utf8').trim()) + '\n');
    } else throw new Error('Invalid release helper command');
  } catch (error) {
    console.error('Release artifact: ' + error.message);
    process.exitCode = 1;
  }
}
