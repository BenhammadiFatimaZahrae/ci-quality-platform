# Simulateur d'evenements CI/CD -> Kafka (topics builds et tests)
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9094')
SEED = int(os.getenv('SIM_SEED', '42'))
BUILDS_PER_MIN = float(os.getenv('SIM_BUILDS_PER_MIN', '30'))
MAX_BUILDS = int(os.getenv('SIM_MAX_BUILDS', '0'))  # 0 = sans fin
DRY_RUN = os.getenv('SIM_DRY_RUN', '0') == '1'  # 1 = affiche sans envoyer

PROJECT_FAIL_RATE = {'repo-A': 0.05, 'repo-B': 0.30, 'repo-C': 0.15,
                     'repo-D': 0.10, 'repo-E': 0.20}
ALL_TESTS = ['test_%02d' % i for i in range(1, 21)]
FLAKY_TESTS = ['test_07', 'test_13', 'test_18']
STABLE_TESTS = [t for t in ALL_TESTS if t not in FLAKY_TESTS]
FLAKY_FAIL_P = 0.25
DUP_P = 0.02
RERUN_P = 0.25

rng = random.Random(SEED)          # commits, builds, durees
dup_rng = random.Random(SEED + 1)  # doublons

producer = None if DRY_RUN else Producer(
    {'bootstrap.servers': BOOTSTRAP, 'acks': 'all', 'linger.ms': 20})
counters = {'sent': 0, 'errors': 0}
build_counter = 0


def now_iso(dt=None):
    return (dt or datetime.now(timezone.utc)).isoformat(timespec='milliseconds')


def on_delivery(err, msg):
    if err is not None:
        counters['errors'] += 1
        print('ERREUR livraison :', err)
    else:
        counters['sent'] += 1


def send(topic, key, event):
    value = json.dumps(event)
    copies = 2 if dup_rng.random() < DUP_P else 1
    for _ in range(copies):
        if DRY_RUN:
            print(topic, key, value)
        else:
            producer.produce(topic, key=key, value=value,
                             on_delivery=on_delivery)
            producer.poll(0)


def new_commit():
    project = rng.choice(list(PROJECT_FAIL_RATE))
    files = min(40, max(1, int(rng.expovariate(1 / 6))))
    p_bad = min(0.9, PROJECT_FAIL_RATE[project] + 0.01 * files)
    bad = rng.sample(STABLE_TESTS, rng.randint(1, 3)) if rng.random() < p_bad else []
    branch = 'main' if rng.random() < 0.4 else rng.choice(
        ['feature/login', 'feature/api', 'fix/bug'])
    return {'sha': '%07x' % rng.getrandbits(28), 'project': project,
            'branch': branch, 'files': files,
            'lines': files * rng.randint(5, 40), 'bad_tests': bad}


def new_build(commit, trigger):
    global build_counter
    build_counter += 1
    duration = max(30, int(rng.gauss(200 + 5 * commit['files'], 20)))
    if rng.random() < 0.05:
        duration *= 2
    return {'build_id': 'B-%06d' % build_counter, 'commit': commit,
            'trigger': trigger, 'started': datetime.now(timezone.utc),
            'duration': duration,
            'result': 'failure' if commit['bad_tests'] else 'success'}


def build_event(b, final):
    c = b['commit']
    finished = b['started'] + timedelta(seconds=b['duration'])
    return {'build_id': b['build_id'], 'project': c['project'],
            'branch': c['branch'], 'commit_sha': c['sha'],
            'trigger': b['trigger'],
            'status': b['result'] if final else 'running',
            'started_at': now_iso(b['started']),
            'finished_at': now_iso(finished) if final else None,
            'duration_s': b['duration'] if final else None,
            'files_changed': c['files'], 'lines_changed': c['lines'],
            'event_ts': now_iso()}


def emit_tests(b):
    c = b['commit']
    trng = random.Random('%d-%s' % (SEED, b['build_id']))  # resultats reproductibles
    for name in ALL_TESTS:
        if name in c['bad_tests']:
            status = 'fail'
        elif name in FLAKY_TESTS and trng.random() < FLAKY_FAIL_P:
            status = 'fail'
        elif trng.random() < 0.02:
            status = 'skipped'
        else:
            status = 'pass'
        send('tests', c['project'], {
            'test_id': b['build_id'] + ':' + name, 'build_id': b['build_id'],
            'project': c['project'], 'commit_sha': c['sha'],
            'test_name': name, 'status': status,
            'duration_ms': trng.randint(20, 2000), 'event_ts': now_iso()})


def finish(b):
    send('builds', b['commit']['project'], build_event(b, final=True))
    emit_tests(b)


def main():
    pending, recent, created = [], [], 0
    print('Simulateur demarre. Kafka =', BOOTSTRAP, '| dry-run =', DRY_RUN)
    try:
        while MAX_BUILDS == 0 or created < MAX_BUILDS:
            if recent and rng.random() < RERUN_P:
                commit, trigger = rng.choice(recent), 'rerun'
            else:
                commit, trigger = new_commit(), 'push'
                recent.append(commit)
                recent[:] = recent[-20:]
            b = new_build(commit, trigger)
            created += 1
            send('builds', commit['project'], build_event(b, final=False))
            pending.append((time.time() + rng.uniform(2, 6), b))
            now = time.time()
            for _, done in [p for p in pending if p[0] <= now]:
                finish(done)
            pending = [p for p in pending if p[0] > now]
            if not DRY_RUN:
                producer.poll(0)
            if created % 50 == 0:
                print('builds crees :', created, counters)
            time.sleep(60.0 / BUILDS_PER_MIN)
    except KeyboardInterrupt:
        print('Arret demande.')
    for _, done in pending:
        finish(done)
    if producer is not None:
        producer.flush(15)
    print('Termine. builds crees :', created, counters)


if __name__ == '__main__':
    main()
