// Pages of the earlier site, by their address within a version. Every language of a build gets
// the same set.
const pages: Record<string, string> = {
  // The crate's guide of the earliest site, which later sent its readers to docs.rs; the guides
  // teach its topics now, and their overview is the front page of the version.
  pubsub: '/',
  // The benchmarks explain what the architecture costs.
  benchmarks: '/architecture/benchmarks/',
};

/** Old address to its target, relative to the build's base URL. */
export function redirects(): Record<string, string> {
  return {...pages};
}
