const fs = require('node:fs');
const os = require('node:os');

// System utilisation, sampled cheaply enough that the load balancer can poll it
// several times a second.
//
// The obvious implementation — diff os.cpus() between two readings — is wrong
// inside a container. os.cpus() describes the host, so on the lab machine it
// averages over all 120 of its cores while this container is entitled to one.
// A backend pinned at its limit would report under 1% busy, which is useless
// both for routing decisions and for the utilisation figures in the report.
//
// The cgroup accounts for what this container actually consumed, so that is
// what gets read, normalised against the container's own CPU quota. The
// os.cpus() path stays as a fallback for running outside a container.

const CGROUP_V2_USAGE = '/sys/fs/cgroup/cpu.stat';
const CGROUP_V1_USAGE = '/sys/fs/cgroup/cpuacct/cpuacct.usage';

// Microseconds of CPU time this container has used since it started.
function readCgroupUsageMicros() {
  try {
    const text = fs.readFileSync(CGROUP_V2_USAGE, 'utf8');
    const match = text.match(/usage_usec\s+(\d+)/);
    if (match) return Number(match[1]);
  } catch {}

  try {
    // cgroup v1 reports nanoseconds.
    const nanos = Number(fs.readFileSync(CGROUP_V1_USAGE, 'utf8').trim());
    if (Number.isFinite(nanos)) return nanos / 1000;
  } catch {}

  return null;
}

// How many CPUs this container may use, which is what "100% busy" means here.
function quotaCores() {
  try {
    const [quota, period] = fs.readFileSync('/sys/fs/cgroup/cpu.max', 'utf8').trim().split(/\s+/);
    if (quota !== 'max') {
      const cores = Number(quota) / Number(period);
      if (cores > 0) return cores;
    }
  } catch {}

  try {
    const quota = Number(fs.readFileSync('/sys/fs/cgroup/cpu/cpu.cfs_quota_us', 'utf8'));
    const period = Number(fs.readFileSync('/sys/fs/cgroup/cpu/cpu.cfs_period_us', 'utf8'));
    if (quota > 0 && period > 0) return quota / period;
  } catch {}

  return os.cpus().length;
}

const CORES = quotaCores();

function hostSnapshot() {
  const cpus = os.cpus();
  let idle = 0;
  let total = 0;

  for (const cpu of cpus) {
    for (const kind of Object.keys(cpu.times)) total += cpu.times[kind];
    idle += cpu.times.idle;
  }

  return { idle, total };
}

let previousUsage = readCgroupUsageMicros();
let previousHost = previousUsage === null ? hostSnapshot() : null;
let previousAt = Date.now();
let cachedPercent = 0;

// Node's own share of the container's CPU. The container figure includes
// everything running in it — most importantly mongod, which sits on the same
// single core — so comparing the two says whether time is going to the
// application or to the database underneath it.
let previousProcess = process.cpuUsage();
let cachedProcessPercent = 0;

// Recomputed on a timer so a burst of /stats requests all read the same recent
// value rather than each taking its own near-zero-length sample.
function sample() {
  const now = Date.now();
  const elapsedMicros = (now - previousAt) * 1000;
  if (elapsedMicros <= 0) return cachedPercent;

  const processUsage = process.cpuUsage();
  const processMicros =
    processUsage.user - previousProcess.user + (processUsage.system - previousProcess.system);
  previousProcess = processUsage;
  cachedProcessPercent = clamp((100 * processMicros) / (elapsedMicros * CORES));

  const usage = readCgroupUsageMicros();

  if (usage !== null && previousUsage !== null) {
    const usedMicros = usage - previousUsage;
    previousUsage = usage;
    cachedPercent = clamp((100 * usedMicros) / (elapsedMicros * CORES));
  } else if (previousHost) {
    const current = hostSnapshot();
    const idleDelta = current.idle - previousHost.idle;
    const totalDelta = current.total - previousHost.total;
    previousHost = current;
    if (totalDelta > 0) {
      cachedPercent = clamp(100 * (1 - idleDelta / totalDelta));
    }
  }

  previousAt = now;
  return cachedPercent;
}

function clamp(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, value));
}

// What the container is actually using, and what it is actually allowed.
//
// os.totalmem()/freemem() describe the host (128GB here) while the cgroup
// enforces 512MB, so the only numbers that predict an OOM kill are these. They
// cover everything in the container — this process, mongod, page cache — which
// is the point: the process that gets killed is chosen by the total, not by any
// one runtime's own accounting.
// Everything the cgroup is charging this container, and how much of it the
// kernel would actually have to kill something to reclaim.
//
// memory.current is not the working set. It includes the page cache — on these
// machines, mongod's data files — which the kernel drops for free under
// pressure. Measured on an idle backend with no messages stored, memory.current
// read 392MB of 512MB, while the two processes together held about 234MB: the
// rest was reclaimable cache. Treating that as pressure would have the backend
// shedding buffers at idle, which costs throughput to relieve a shortage that
// does not exist.
//
// So the figure that drives shedding subtracts what the kernel can take back
// without killing anything: inactive file pages and reclaimable slab. What is
// left is anonymous memory and kernel structures, which is what an OOM kill is
// actually made of.
function readMemoryStat() {
  try {
    const text = fs.readFileSync('/sys/fs/cgroup/memory.stat', 'utf8');
    const out = {};
    for (const line of text.split('\n')) {
      const space = line.indexOf(' ');
      if (space > 0) out[line.slice(0, space)] = Number(line.slice(space + 1));
    }
    return out;
  } catch {
    return null;
  }
}

function containerMemory() {
  const read = (path) => {
    try {
      return fs.readFileSync(path, 'utf8').trim();
    } catch {
      return null;
    }
  };

  const rawCurrent = read('/sys/fs/cgroup/memory.current');
  const rawMax = read('/sys/fs/cgroup/memory.max');
  if (rawCurrent === null || rawMax === null || rawMax === 'max') return null;

  const current = Number(rawCurrent);
  const max = Number(rawMax);
  if (!Number.isFinite(current) || !Number.isFinite(max) || max <= 0) return null;

  const stat = readMemoryStat();
  const reclaimable = stat
    ? (stat.inactive_file || 0) + (stat.slab_reclaimable || 0)
    : 0;
  // Never let a bad reading make the working set look larger than the total.
  const workingSet = Math.max(0, Math.min(current, current - reclaimable));

  return {
    usedMb: Math.round(current / 1048576),
    limitMb: Math.round(max / 1048576),
    usedPercent: Number(((100 * current) / max).toFixed(1)),
    // What the kernel cannot reclaim without killing a process. This is the
    // number that predicts an OOM kill, and the one shedding responds to.
    workingSetMb: Math.round(workingSet / 1048576),
    workingSetPercent: Number(((100 * workingSet) / max).toFixed(1)),
    reclaimableMb: Math.round(reclaimable / 1048576),
  };
}

function getStats(extra = {}) {
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const container = containerMemory();

  return {
    hostname: os.hostname(),
    pid: process.pid,
    // Percent of *this container's* CPU entitlement, not of the host.
    cpuPercent: Number(cachedPercent.toFixed(2)),
    // Node's own share; the gap between this and cpuPercent is mongod and
    // anything else sharing the container.
    processCpuPercent: Number(cachedProcessPercent.toFixed(2)),
    cpuCores: Number(CORES.toFixed(2)),
    hostCpuCount: os.cpus().length,
    loadAvg1: Number(os.loadavg()[0].toFixed(2)),
    memUsedPercent: Number((100 * (1 - freeMem / totalMem)).toFixed(2)),
    memTotalMb: Math.round(totalMem / 1024 / 1024),
    heapUsedMb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
    rssMb: Math.round(process.memoryUsage().rss / 1024 / 1024),
    // The number that predicts an OOM kill. null outside a container.
    container,
    uptimeSeconds: Math.round(process.uptime()),
    ...extra,
  };
}

// Fraction of the container's memory budget in use, 0-1, or null if unknown.
// The store consults this to shed memory before the kernel does it for us.
function memoryPressure() {
  const container = containerMemory();
  return container ? container.workingSetPercent / 100 : null;
}

function startSampling(intervalMs = 500) {
  const timer = setInterval(sample, intervalMs);
  if (typeof timer.unref === 'function') timer.unref();
  return timer;
}

module.exports = { getStats, startSampling, sample, quotaCores, memoryPressure, containerMemory };
